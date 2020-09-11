import pyodbc
import pandas as pd
import numpy as np
import datetime
import keyring
import sys
import logging
from dateutil.relativedelta import relativedelta
import time
logging.basicConfig(filename= 'run.log', level = logging.ERROR) #loggin設定
pd.set_option('display.max_columns', None)


'''
Function Zone
'''
def Get_Noah_Site(BalanceCenter_190):
    Noah1_query = "SELECT [Siteid]\
                   FROM [DataScientist].[dbo].[DS_BalanceCenterDailyQuery_NoahSite]\
                   WHERE [SiteType] = 'Noah1'" 

    Noah2_query = "SELECT [Siteid]\
                   FROM [DataScientist].[dbo].[DS_BalanceCenterDailyQuery_NoahSite]\
                   WHERE [SiteType] = 'Noah2'" 
    
    Noah1_Siteid, Noah2_Siteid = BalanceCenter_190.ExecQuery(Noah1_query), BalanceCenter_190.ExecQuery(Noah2_query)
    
    return Noah1_Siteid, Noah2_Siteid



def Get_Fliter_Site(BalanceCenter_190, Condition_Exclude_Site):
    if Condition_Exclude_Site == 'NotExcludeNoah1Noah2':
        
        query = "SELECT [Siteid]\
                 FROM [DataScientist].[dbo].[DS_BalanceCenterDailyQuery_NoahSite]\
                 WHERE [SiteType] is null"
                 
    elif Condition_Exclude_Site == 'ExcludeNoah1':
        
        query = "SELECT [Siteid]\
                 FROM [DataScientist].[dbo].[DS_BalanceCenterDailyQuery_NoahSite]\
                 WHERE [SiteType] = 'Noah1'"      
                 
    elif Condition_Exclude_Site == 'ExcludeNoah1Noah2':
        
        query = "SELECT [Siteid]\
                 FROM [DataScientist].[dbo].[DS_BalanceCenterDailyQuery_NoahSite]\
                 where [SiteType] in ('Noah1', 'Noah2')"
        
    Fliter_site = BalanceCenter_190.ExecQuery(query)    
    
    return Fliter_site


def Insert_Status_Fail_FromJG(JG, JG_lookuptablename, current, serverip, linkserver, StatsTable_bytype):
    df = JG.ExecQuery("select '{serverip}' as [Server], Type, 'Fail' as [Status],'{time}' as [UpDateTime] from  {table}\
                      where [LinkServerName] = '{linkserver}'\
                      group by Type".format(table=JG_lookuptablename,
                                            time=current,
                                            serverip=serverip,
                                            linkserver=linkserver))
    JG.Executemany("insert into {Table}(\
                   [Server], [Type], [Status], [UpDateTime]) \
                    values (?,?,?,?)".format(Table=StatsTable_bytype), df)


def Insert_Status_Fail_FromSuccessMax(ConnectSQL_RecordStatus, current, serverip, StatsTable_bytype):
    
    ConnectSQL_RecordStatus.ExecNoQuery("insert into {StatsTable_bytype}\
                                         SELECT [Server]\
                                               ,[Type]\
                                               ,[Destination]\
                                               ,'Fail' as [Status]\
                                               ,[Exe_Time_sec]\
                                               ,'{current}' as [UpDateTime]\
                                               FROM {StatsTable_bytype}\
                                         where server = '{serverip}' and [UpDateTime] = (select max([UpDateTime])\
													                                     from {StatsTable_bytype}\
													                                     where server = '{serverip}' \
													                                           and [Status] in ('Success', 'Empty'))".format(StatsTable_bytype=StatsTable_bytype,
                                                                                                                                             current=current,
                                                                                                                                             serverip=serverip))
    
    
    



def String_OR(df):
    temp = df.copy()
    temp['Condition'] = temp.apply(lambda row: "(server = '{}'\
                                                and type = {}\
                                                and updatetime = '{}'\
                                                and Destination = '{}')".format(row['Server'], row['Type'], row['UpDateTime'], row['Destination']), axis=1)
    S = temp.Condition.tolist()
    string = S[0]
    S = S[1:]
    for elem in S:
        string += ' or {}'.format(elem)
    return string



#讀取對照表
def select_from_sql(now, current_day, linkserver, server, JG):
    def get_target():
        try:
            #chart = server.ExecQuery("select * from [BalanceOutcome].[dbo].[LookUpTable] where dbname != '_CasinoBalanceCenter_BalanceAs'")
            chart = server.ExecQuery("select * from [BalanceOutcome].[dbo].[LookUpTableNew]")
        except:
            #IP191 = Duizhang(191)
            chart = JG.ExecQuery("select * from [BalanceOutcome].[dbo].[LookUpTable]")

        try:
            Exclude_rawdatatype_df = server.ExecQuery("SELECT distinct [RawDataType] Type\
                                                       FROM [DataPool].[dbo].[VW_GameLookup]\
                                                       where [GameHallName] in ('SE','3Sing','TGP2','PS2')")
            Exclude_rawdatatype = list(Exclude_rawdatatype_df.Type)
        except:
            Exclude_rawdatatype = [35, 67, 68, 139]# Mako said that there aren't unique gameaccount in these gamehall(rawdatatype)
        
        chart.columns = ['LinkServerName', 'GameTypeSourceId', 'DBName', 'MonthDB', 'TableName', 'Type']    
        

        #fliter
        target = chart[~chart['GameTypeSourceId'].isin(Exclude_rawdatatype)].copy()
        target = target[target['LinkServerName'] == linkserver].reset_index(drop=True)
        
        return target
    
    def group_by_type(target):
        groupbytype = target.drop(['GameTypeSourceId'],axis=1)
        groupbytype = groupbytype.drop_duplicates().reset_index(drop=True)
       
        zmm = target.groupby(['Type'], as_index=True)['GameTypeSourceId'].apply(lambda x: tuple(x.values) if len(x)>1 else x.iloc[0])
        game_lookup_tuple = pd.DataFrame({'Type':zmm.index,
                                          'Game_list':zmm}).reset_index(drop=True)        
        groupbytype = groupbytype.merge(game_lookup_tuple,
                                        how = 'left',
                                        on = 'Type')
        
        return groupbytype

    
    def SQL_timezone_sqlstring(row):
        DBName_sql = row['DBName_sql']
        RawDataType = row['Type']
        return "SELECT [Timezone] FROM {DB}.[dbo].[VW_RawDataInfo] where type = {type}".format(DB = DBName_sql, type = RawDataType)

    
    def SQL_EndTime(row, nowutc):
        Delta = row['timezone_value']
        End_time = (datetime.datetime.strptime(nowutc, "%Y-%m-%d %H:00:00.000") + datetime.timedelta(hours = Delta)).strftime("%Y-%m-%d %H:00:00.000")
        return End_time
    
    def SQL_StartTime(row, delta):
        End_time = row['End_Table_time']
        Start_time = (datetime.datetime.strptime(End_time, "%Y-%m-%d %H:00:00.000") + datetime.timedelta(hours = delta)).strftime("%Y-%m-%d %H:00:00.000")
        return Start_time

    def SQL_RealMonth(row):
        start = row['Start_Table_time']
        RealMonth= str(start[2:4]+start[5:7])
        return RealMonth
    
    def SQL_query_bytype(row, current_day):
        
        original_dbname = row['DBName']
        LinkServerName = row['LinkServerName']
        game_list = row['Game_list']
        if type(game_list) == tuple:
            game_list = game_list
        else:
            game_list = "(" + str(game_list)+ ")"
        
        current_db = row['DBName_sql_end']
        #previous_db = row['DBName_sql_start']
        table_name = 'VW_'+row['TableName']
        
        endtime = row['End_Table_time']
        starttime = row['Start_Table_time']

        if original_dbname == "_CasinoBalanceCenter_BalanceC":
            sqlquery = "SELECT '{date}' [Dateplayed],\
                                lower([GameAccount]) GameAccount, \
                                [SiteId],\
                                [GameTypeSourceId], \
                                Isnull(Sum([betamount]), 0) [Commissionable],\
                                Count(1) [WagersCount], \
                                Isnull(sum([PayOff]), 0) [PayOff],\
                                sum(case when Isnull([PayOff], 0) > 0 then 1 else 0 end) [WinsCount] \
                                FROM {LinkServerName}.{current_db}.dbo.{table_name} with (nolock) \
                        WHERE [wagerstime] >= '{starttime}' \
                              AND [wagerstime] < '{endtime}' \
                              AND GameTypeSourceId in {Game}\
                        GROUP BY lower([gameaccount]), [siteid], [GameTypeSourceId] \
                        HAVING Sum([betamount]) > 0".format(LinkServerName=LinkServerName,
                                                            date=current_day,
                                                            Game=game_list,                                                     
                                                            current_db=current_db,
                                                            table_name=table_name,
                                                            endtime=endtime,
                                                            starttime=starttime)
        else:
            sqlquery = "SELECT '{date}' [Dateplayed],\
                                lower([GameAccount]) [GameAccount],\
                                [SiteId],\
                                [GameTypeSourceId], \
                                Isnull(Sum([betamount]), 0) [Commissionable],\
                                Count(1) [WagersCount],\
                                Isnull(sum([PayOff]), 0) [PayOff],\
                                sum(case when Isnull([PayOff], 0) > 0 then 1 else 0 end) [WinsCount]\
                        FROM {LinkServerName}.{current_db}.dbo.{table_name} with (nolock) \
                        WHERE [wagerstime] >= '{starttime}' AND [wagerstime] < '{endtime}' \
                        GROUP BY lower([gameaccount]), [siteid], [GameTypeSourceId] \
                        HAVING Sum([betamount]) > 0".format(LinkServerName=LinkServerName,
                                                            date=current_day,
                                                            current_db=current_db,
                                                            table_name=table_name,
                                                            endtime=endtime,
                                                            starttime=starttime)
        return sqlquery        
    
    target = get_target()
    groupbytype = group_by_type(target)

    timezone_string = "select Type, Timezone from DataScientist.[dbo].[DS_DuizhangTimeZone]"
    timezone_df = JG.ExecQuery(timezone_string)
    timezone_df.columns = ['Type', 'timezone_value']    
    
    groupbytype = groupbytype.merge(timezone_df[['Type', 'timezone_value']],
                                    how = 'left',
                                    on = 'Type')  
    
    groupbytype.loc[:, 'End_Table_time'] = groupbytype.apply(lambda row: SQL_EndTime(row, nowutc = now), axis=1)
    groupbytype.loc[:, 'Start_Table_time'] = groupbytype.apply(lambda row: SQL_StartTime(row, delta = -1), axis=1)            
    groupbytype.loc[:, 'DBName_sql_end'] = groupbytype.apply(lambda row: 
                                            str(row['DBName'])
                                            + row['End_Table_time'][2:4]
                                            + row['End_Table_time'][5:7] if row['MonthDB'] else row['DBName'], axis=1)
    groupbytype.loc[:, 'DBName_sql_start'] = groupbytype.apply(lambda row: 
                                            str(row['DBName'])
                                            + row['Start_Table_time'][2:4]
                                            + row['Start_Table_time'][5:7] if row['MonthDB'] else row['DBName'], axis=1)    
    
    #added by jimmy yin on 2020/05/14, due to lookuptable changed.
    groupbytype.loc[:, 'RealMonth'] = groupbytype.apply(lambda row: SQL_RealMonth(row), axis=1) 
    groupbytype_fliter = groupbytype[(groupbytype.MonthDB == groupbytype.RealMonth) | (groupbytype.MonthDB.isnull()) ].reset_index(drop=True)
    groupbytype_fliter.loc[:, 'Sqlquery'] = groupbytype_fliter.apply(lambda row: SQL_query_bytype(row, current_day), axis=1)
        
    return groupbytype_fliter

'''
#讀取對照表
def select_from_sql(now, current_day, linkserver, server, JG):
    def get_target():
        try:
            #chart = server.ExecQuery("select * from [BalanceOutcome].[dbo].[LookUpTable] where dbname != '_CasinoBalanceCenter_BalanceAs'")
            chart = server.ExecQuery("select * from (\
                                    SELECT lk.*\
                                    FROM   [BalanceOutcome].[dbo].[lookuptable] lk \
                                    INNER JOIN (SELECT [gametypesourceid] \
                                                FROM   [BalanceOutcome].[dbo].[lookuptable] \
                                                 GROUP  BY [gametypesourceid]\
                                                       HAVING Count(1) = 1) fliter \
                                                   ON lk.gametypesourceid = fliter.gametypesourceid\
                                    union all\
                                    SELECT lk.*\
                                    FROM   [BalanceOutcome].[dbo].[lookuptable] lk \
                                    INNER JOIN (SELECT [gametypesourceid] \
                                                FROM   [BalanceOutcome].[dbo].[lookuptable] \
                                                 GROUP  BY [gametypesourceid]\
                                                       HAVING Count(1) >= 2) fliter \
                                                   ON lk.gametypesourceid = fliter.gametypesourceid\
                                    			      where lk.dbname !=  '_CasinoBalanceCenter_BalanceA')y\
                                    order by gametypesourceid")
        except:
            #IP191 = Duizhang(191)
            chart = JG.ExecQuery("select * from [BalanceOutcome].[dbo].[LookUpTable]")

        try:
            Exclude_rawdatatype_df = server.ExecQuery("SELECT distinct [RawDataType] Type\
                                                       FROM [DataPool].[dbo].[VW_GameLookup]\
                                                       where [GameHallName] in ('SE','3Sing','TGP2','PS2')")
            Exclude_rawdatatype = list(Exclude_rawdatatype_df.Type)
        except:
            Exclude_rawdatatype = [35, 67, 68, 139]# Mako said that there aren't unique gameaccount in these gamehall(rawdatatype)
        
        chart.columns = ['LinkServerName', 'GameTypeSourceId', 'DBName', 'MonthDB', 'TableName', 'Type']    
        

        #fliter
        target = chart[~chart['GameTypeSourceId'].isin(Exclude_rawdatatype)].copy()
        target = target[target['LinkServerName'] == linkserver]
        

        target['DBName_sql'] = target.apply(lambda row: 
            str(row['DBName'])
            + now[2:4]
            + now[5:7] if row['MonthDB'] else row['DBName'], axis=1)

        previous_month = (datetime.datetime.strptime(now, "%Y-%m-%d %H:00:00.000") 
        - relativedelta(months=1)).strftime("%Y-%m-%d %H:00:00.000")
        target['DBName_sql_previous_month'] = target.apply(lambda row: 
            str(row['DBName'])
            + previous_month[2:4]
            + previous_month[5:7] if row['MonthDB'] else row['DBName'], axis=1)
        
        target = target.reset_index(drop=True)
        
        return target
    
    def group_by_type(target):
        groupbytype = target.drop(['GameTypeSourceId'],axis=1)
        groupbytype = groupbytype.drop_duplicates().reset_index(drop=True)
       
        zmm = target.groupby(['Type'], as_index=True)['GameTypeSourceId'].apply(lambda x: tuple(x.values) if len(x)>1 else x.iloc[0])
        game_lookup_tuple = pd.DataFrame({'Type':zmm.index,
                                          'Game_list':zmm}).reset_index(drop=True)        
        groupbytype = groupbytype.merge(game_lookup_tuple,
                                        how = 'left',
                                        on = 'Type')
        
        return groupbytype

    
    def SQL_timezone_sqlstring(row):
        DBName_sql = row['DBName_sql']
        RawDataType = row['Type']
        return "SELECT [Timezone] FROM {DB}.[dbo].[VW_RawDataInfo] where type = {type}".format(DB = DBName_sql, type = RawDataType)

    
    def SQL_EndTime(row, nowutc):
        Delta = row['timezone_value']
        End_time = (datetime.datetime.strptime(nowutc, "%Y-%m-%d %H:00:00.000") + datetime.timedelta(hours = Delta)).strftime("%Y-%m-%d %H:00:00.000")
        return End_time
    
    def SQL_StartTime(row, delta):
        End_time = row['End_Table_time']
        Start_time = (datetime.datetime.strptime(End_time, "%Y-%m-%d %H:00:00.000") + datetime.timedelta(hours = delta)).strftime("%Y-%m-%d %H:00:00.000")
        return Start_time
    
    def SQL_query_bytype(row, current_day):
        
        original_dbname = row['DBName']
        LinkServerName = row['LinkServerName']
        game_list = row['Game_list']
        if type(game_list) == tuple:
            game_list = game_list
        else:
            game_list = "(" + str(game_list)+ ")"
        
        current_db = row['DBName_sql_end']
        #previous_db = row['DBName_sql_start']
        table_name = 'VW_'+row['TableName']
        
        endtime = row['End_Table_time']
        starttime = row['Start_Table_time']

        if original_dbname == "_CasinoBalanceCenter_BalanceC":
            sqlquery = "SELECT '{date}' [Dateplayed], lower([GameAccount]) GameAccount, [SiteId], [GameTypeSourceId], \
                        Isnull(Sum([betamount]), 0) [Commissionable], Count(1) [WagersCount], Isnull(sum([PayOff]), 0) [PayOff],\
                        sum(case when Isnull([PayOff], 0) > 0 then 1 else 0 end) [WinsCount] FROM\
                        {LinkServerName}.{current_db}.dbo.{table_name} with (nolock) \
                        WHERE [wagerstime] >= '{starttime}' AND [wagerstime] < '{endtime}' \
                        AND GameTypeSourceId in {Game}\
                        GROUP BY lower([gameaccount]), [siteid], [GameTypeSourceId] HAVING Sum([betamount]) > 0".format(LinkServerName=LinkServerName,
                                                                                                                 date=current_day,
                                                                                                                 Game=game_list,                                                     
                                                                                                                 current_db=current_db,
                                                                                                                 table_name=table_name,
                                                                                                                 endtime=endtime,
                                                                                                                 starttime=starttime)
        else:
            sqlquery = "SELECT '{date}' [Dateplayed], lower([GameAccount]) [GameAccount], [SiteId], [GameTypeSourceId], \
                        Isnull(Sum([betamount]), 0) [Commissionable], Count(1) [WagersCount], Isnull(sum([PayOff]), 0) [PayOff],\
                        sum(case when Isnull([PayOff], 0) > 0 then 1 else 0 end) [WinsCount]\
                        FROM\
                        {LinkServerName}.{current_db}.dbo.{table_name} with (nolock) \
                        WHERE [wagerstime] >= '{starttime}' AND [wagerstime] < '{endtime}' \
                        GROUP BY lower([gameaccount]), [siteid], [GameTypeSourceId] HAVING Sum([betamount]) > 0".format(LinkServerName=LinkServerName,
                                                                                                                 date=current_day,
                                                                                                                 current_db=current_db,
                                                                                                                 table_name=table_name,
                                                                                                                 endtime=endtime,
                                                                                                                 starttime=starttime)
        return sqlquery        
    
    target = get_target()
    groupbytype = group_by_type(target)

    timezone_string = "select Type, Timezone from DataScientist.[dbo].[DS_DuizhangTimeZone]"
    timezone_df = JG.ExecQuery(timezone_string)
    timezone_df.columns = ['Type', 'timezone_value']    
    
    groupbytype = groupbytype.merge(timezone_df[['Type', 'timezone_value']],
                                    how = 'left',
                                    on = 'Type')  
    
    groupbytype.loc[:, 'End_Table_time'] = groupbytype.apply(lambda row: SQL_EndTime(row, nowutc = now), axis=1)
    groupbytype.loc[:, 'Start_Table_time'] = groupbytype.apply(lambda row: SQL_StartTime(row, delta = -1), axis=1)            
    groupbytype.loc[:, 'DBName_sql_end'] = groupbytype.apply(lambda row: 
                                            str(row['DBName'])
                                            + row['End_Table_time'][2:4]
                                            + row['End_Table_time'][5:7] if row['MonthDB'] else row['DBName'], axis=1)
    groupbytype.loc[:, 'DBName_sql_start'] = groupbytype.apply(lambda row: 
                                            str(row['DBName'])
                                            + row['Start_Table_time'][2:4]
                                            + row['Start_Table_time'][5:7] if row['MonthDB'] else row['DBName'], axis=1)    
    
    groupbytype.loc[:, 'Sqlquery'] = groupbytype.apply(lambda row: SQL_query_bytype(row, current_day), axis=1)
        
    return groupbytype
'''

#def update_string(sql_process, serverip, current, Type_forRecord, StatsTable_bytype, status='Success'):




def Record_By_Type(**arg):
    
    SQLQuery_df, Server = arg['ProcessDf'], arg['Server']
    current, Record_server = arg['Time'], arg['Connect_RecordServer']
    StatusTable, Section = arg['StatusTable'], arg['Section']

    if Section == 'FromGPKBalanceCenterToDSServer':
        Running_bytype = pd.DataFrame({'Server':Server,
                                       'Type':np.array(SQLQuery_df.Type),
                                       'Destination': 'DS_Server',
                                       'Status':'Running',
                                       'UpDateTime':current})                    
    elif Section == 'FromNoahBalanceCenterToDSServerNoahServer':
        Running_bytype = pd.DataFrame({'Server':Server,
                                       'Type':np.array(SQLQuery_df.Type), 
                                       'Destination': 'DS_Server',
                                       'Status':'Running',
                                       'UpDateTime':current})      
        Running_bytype_noah1 = pd.DataFrame({'Server':Server,
                                       'Type':np.array(SQLQuery_df.Type), 
                                       'Destination': 'Noah1_Server',
                                       'Status':'Running',
                                       'UpDateTime':current})  
        Running_bytype_noah2 = pd.DataFrame({'Server':Server,
                                       'Type':np.array(SQLQuery_df.Type), 
                                       'Destination': 'Noah2_Server',
                                       'Status':'Running',
                                       'UpDateTime':current})         
        Running_bytype = pd.concat([Running_bytype, Running_bytype_noah1, Running_bytype_noah2],
                                   axis=0).reset_index(drop=True)
      

    Record_server.Executemany("insert into {Table}(\
                               [Server], [Type], [Destination], [Status], [UpDateTime]) \
                               values (?,?,?,?,?)".format(Table=StatusTable),
                               Running_bytype)
                
    


#Processing
''' 
def process(SQLQuery_df, IP, JG, serverip, current, DailyQueryTable, DailyQueryTable_190, DataNotReady, Fliter_site, StatsTable_bytype): 
    for i in range(SQLQuery_df.shape[0]):
        #print(i)
        sqlquery = SQLQuery_df['Sqlquery'][i]  
        Type_forRecord = SQLQuery_df['Type'][i]
        sql_start = time.time()
        try: 
            if DataNotReady[DataNotReady.Type.isin([Type_forRecord])].empty:
                
                df = IP.ExecQuery(sqlquery)
                df = df[~df.SiteId.isin(Fliter_site.Siteid)].reset_index(drop=True)

                
                if not df.empty:#Success
                    #print('Success')    
                    IP.Executemany("insert into {table}(\
                                   [DatePlayed], [GameAccount], [SiteId], [GameTypeSourceId], [Commissionable], [WagersCount], [Payoff], [WinsCount]) \
                                    values (?,?,?,?,?,?,?,?)".format(table=DailyQueryTable_190), df)      
                      
                    sql_process = (time.time()- sql_start)

                    UpdateSQLString = update_string(ProcessTime=sql_process,
                                                    serverip=serverip, 
                                                    current=current, 
                                                    Type_forRecord=Type_forRecord,
                                                    Table=StatsTable_bytype,
                                                    status='Success')                                                                                                                                                                                 
                    JG.ExecNoQuery( UpdateSQLString )
                    
                else:#Empty
                    #print('Empty')
                    sql_process = (time.time()- sql_start)
                    UpdateSQLString = update_string(ProcessTime=sql_process,
                                                    serverip=serverip, 
                                                    current=current, 
                                                    Type_forRecord=Type_forRecord,
                                                    Table=StatsTable_bytype,
                                                    status='Empty')                                                                                                                                                                                                     
                    JG.ExecNoQuery( UpdateSQLString ) 
                    
            else:  #in data not ready
                sql_process = (time.time()- sql_start)
                UpdateSQLString = update_string(ProcessTime=sql_process,
                                                serverip=serverip, 
                                                current=current, 
                                                Type_forRecord=Type_forRecord,
                                                Table=StatsTable_bytype,
                                                status='Fail')                                                                                                                                                                                                              
                JG.ExecNoQuery( UpdateSQLString )                  

                
        except BaseException as err:#Fail
            print('Fail')
            logging.error('撈取query table, type = {Type_forRecord}時發生錯誤: {err}'.format(Type_forRecord=Type_forRecord, err=str(err)))
            sql_process = (time.time()- sql_start)
            UpdateSQLString = update_string(ProcessTime=sql_process,
                                            serverip=serverip, 
                                            current=current, 
                                            Type_forRecord=Type_forRecord,
                                            Table=StatsTable_bytype,
                                            status='Fail')                                                                                                                                                                               
            JG.ExecNoQuery( UpdateSQLString )                           
            continue
'''   
     
def JudgeLAGDataProcedure(JG_LagDataTable_status, current, ConnectSQL_RecordStatus, sleep_sec=3, last_sec=600):
    last = int(last_sec/sleep_sec)
    for k in range(0, last):
        #print(k)
        sqlquery = "SELECT * FROM {table} \
                    where [dateplayed] = '{current}' and status = 'Success'".format(table=JG_LagDataTable_status,
                                                                                    current=current)
        df = ConnectSQL_RecordStatus.ExecQuery(sqlquery)
        if not df.empty:
            result = 'LAGData Procedure is done'
            return result
            break
        elif((df.empty) & (k == (last-1))):
            result = "LAGData Procedure isn't done yet"
            return result
        else:
            time.sleep(sleep_sec)#stop 3sec, last to 10 min      
    
'''
process(ProcessDf=SQLQuery_df,
        Connect_DSServer=BalanceCenter_DS,
        Connect_Noah1Server=BalanceCenter_Noah1,
        Connect_Noah2Server=BalanceCenter_Noah2,      
        Connect_RecordServer=JG,
        Server=serverip,
        Time=current,
        DailyQueryTable=parameter.DailyQueryTable,
        LagData=DataNotReady,
        Fliter=Fliter_site,
        StatusTable=parameter.StatsTable_bytype,
        Section=Section,
        Noah1_Siteid=Noah1_Siteid,
        Noah2_Siteid=Noah2_Siteid,
        ProductionOrRedo = 'Production',#Redo
        To = 'DS_Server'#['DS_Server', 'Noah1_Server', 'Noah2_Server']
        )


DS_server = BalanceCenter_DS
Noah1_server = BalanceCenter_Noah1
Noah2_server = BalanceCenter_Noah2
Record_server = JG
Server = serverip
'''
#Processing        
#def process(SQLQuery_df, IP, JG, serverip, current, DailyQueryTable, DailyQueryTable_190, DataNotReady, Fliter_site, StatusTable, ):


def process(**arg):
    
    SQLQuery_df = arg['ProcessDf']
    DS_server, Noah1_server, Noah2_server, Record_server,  = arg['Connect_DSServer'], arg['Connect_Noah1Server'],  arg['Connect_Noah2Server'], arg['Connect_RecordServer']
    Server = arg['Server']
    current, DailyQueryTable, DataNotReady, Fliter_site = arg['Time'], arg['DailyQueryTable'], arg['LagData'], arg['Fliter']
    StatusTable, Section = arg['StatusTable'], arg['Section']
    Noah1_Siteid, Noah2_Siteid = arg['Noah1_Siteid'], arg['Noah2_Siteid']
    ProductionOrRedo = 'Production'
    To = arg['To']    

    def update_string(**arg):
    
        ProcessTime, serverip, current = arg['ProcessTime'], arg['serverip'], arg['current']
        Type_forRecord, Table, status = arg['Type_forRecord'], arg['Table'], arg['status']
        Destination = arg['Destination']#['all', 'DS_Server', 'Noah1_Server', 'Noah2_Server']
        
        if Destination == 'all':
            updatestring = "update  {table} \
                            set Status = '{Status}',\
                                Exe_Time_sec = {exe} \
                            WHERE Server = '{serverip}' \
                                  and UpDateTime = '{current}' \
                                  and Type = {Type}".format(table=Table,
                                                            Status=status,
                                                            exe=ProcessTime,
                                                            serverip=serverip,
                                                            current=current,
                                                            Type=Type_forRecord)  
        else:
            updatestring = "update  {table} \
                            set Status = '{Status}',\
                                Exe_Time_sec = {exe} \
                            WHERE Server = '{serverip}' \
                                  and UpDateTime = '{current}' \
                                  and Type = {Type}\
                                  and Destination='{Destination}'".format(table=Table,
                                                                          Status=status,
                                                                          exe=ProcessTime,
                                                                          serverip=serverip,
                                                                          current=current,
                                                                          Type=Type_forRecord,
                                                                          Destination=Destination)                                       
        return updatestring 
   
    def process_query(Section, DS_server, Noah_server, sqlquery, Fliter_site):
        
        if Section == 'FromGPKBalanceCenterToDSServer':
            
            df = DS_server.ExecQuery(sqlquery)
            #df = df[~df.SiteId.isin(Fliter_site.Siteid)].reset_index(drop=True)
            
        elif Section == 'FromNoahBalanceCenterToDSServerNoahServer':
            
            df = Noah_server.ExecQuery(sqlquery)
            #df = df[df.SiteId.isin(Fliter_site.Siteid)].reset_index(drop=True)   
            
        return df 


 
    #def process_insert(df, DailyQueryTable, Section, DS_server, Noah_server, sql_start, Server, current, Type_forRecord, StatusTable, Record_server, Noah1_Siteid, Noah2_Siteid):
    def process_insert(**arg):
        
        df, DailyQueryTable, Section  = arg['df'], arg['DailyQueryTable'], arg['Section']
        DS_server, Noah1_server, Noah2_server = arg['DS_server'], arg['Noah1_server'], arg['Noah2_server']
        sql_start, Server, current = arg['sql_start'], arg['Server'], arg['current']
        Type_forRecord, StatusTable = arg['Type_forRecord'], arg['StatusTable']
        Record_server = arg['Record_server']
        Noah1_Siteid, Noah2_Siteid = arg['Noah1_Siteid'], arg['Noah2_Siteid']
        ProductionOrRedo = arg['ProductionOrRedo']
        To = arg['To']
        
        def process_insert_structure(df, DailyQueryTable, Connect_Server, sql_start, Server, current, Type_forRecord, StatusTable, Record_server, Destination):
            if not df.empty:#Success
                Connect_Server.Executemany("INSERT INTO {table}(\
                                            [DatePlayed], [GameAccount], [SiteId], [GameTypeSourceId],\
                                            [Commissionable], [WagersCount], [Payoff], [WinsCount]) \
                                            VALUES (?,?,?,?,?,?,?,?)".format(table=DailyQueryTable), df)      
                  
                sql_process = (time.time()- sql_start)
    
                UpdateSQLString = update_string(ProcessTime=sql_process,
                                                serverip=Server, 
                                                current=current, 
                                                Type_forRecord=Type_forRecord,
                                                Table=StatusTable,
                                                status='Success',
                                                Destination=Destination)                                                                                                                                                                                 
                Record_server.ExecNoQuery( UpdateSQLString )
                
            else:#Empty
                sql_process = (time.time()- sql_start)
                UpdateSQLString = update_string(ProcessTime=sql_process,
                                                serverip=Server, 
                                                current=current, 
                                                Type_forRecord=Type_forRecord,
                                                Table=StatusTable,
                                                status='Empty',
                                                Destination=Destination)                                                                                                                                                                                                     
                Record_server.ExecNoQuery( UpdateSQLString )
        
        if ProductionOrRedo == 'Production':    
        #insert into ds server 190        
            try:           
                process_insert_structure(df,
                                         DailyQueryTable,
                                         DS_server,
                                         sql_start,
                                         Server,
                                         current,
                                         Type_forRecord, 
                                         StatusTable, 
                                         Record_server,
                                         Destination='DS_Server')
                
            except BaseException as err:#Fail
                logging.error('insert gpk table, type = {Type_forRecord}時發生錯誤: {err}'.format(Type_forRecord=Type_forRecord, err=str(err)))
                sql_process = (time.time()- sql_start)
                UpdateSQLString = update_string(ProcessTime=sql_process,
                                                serverip=Server, 
                                                current=current, 
                                                Type_forRecord=Type_forRecord,
                                                Table=StatusTable,
                                                status='Fail',
                                                Destination='DS_Server')                                                                                                                                                                               
                Record_server.ExecNoQuery( UpdateSQLString )                   
                        
            if Section == 'FromNoahBalanceCenterToDSServerNoahServer':     
                #insert into noah1 server 254
                try:
                    df_noah1 = df[df.SiteId.isin(Noah1_Siteid.Siteid)].reset_index(drop=True)
                    process_insert_structure(df_noah1,
                                             DailyQueryTable,
                                             Noah1_server,
                                             sql_start,
                                             Server,
                                             current,
                                             Type_forRecord, 
                                             StatusTable, 
                                             Record_server,
                                             Destination='Noah1_Server')
                except BaseException as err:#Fail
                    logging.error('insert noah table, type = {Type_forRecord}時發生錯誤: {err}'.format(Type_forRecord=Type_forRecord, err=str(err)))
                    sql_process = (time.time()- sql_start)
                    UpdateSQLString = update_string(ProcessTime=sql_process,
                                                    serverip=Server, 
                                                    current=current, 
                                                    Type_forRecord=Type_forRecord,
                                                    Table=StatusTable,
                                                    status='Fail',
                                                    Destination='Noah1_Server')                                                                                                                                                                               
                    Record_server.ExecNoQuery( UpdateSQLString )  
    
                #insert into noah2 server 170
                try:
                    df_noah2 = df[df.SiteId.isin(Noah2_Siteid.Siteid)].reset_index(drop=True)
                    process_insert_structure(df_noah2,
                                             DailyQueryTable,
                                             Noah2_server,
                                             sql_start,
                                             Server,
                                             current,
                                             Type_forRecord, 
                                             StatusTable, 
                                             Record_server,
                                             Destination='Noah2_Server')
                except BaseException as err:#Fail
                    logging.error('insert noah2 table, type = {Type_forRecord}時發生錯誤: {err}'.format(Type_forRecord=Type_forRecord, err=str(err)))
                    sql_process = (time.time()- sql_start)
                    UpdateSQLString = update_string(ProcessTime=sql_process,
                                                    serverip=Server, 
                                                    current=current, 
                                                    Type_forRecord=Type_forRecord,
                                                    Table=StatusTable,
                                                    status='Fail',
                                                    Destination='Noah2_Server')                                                                                                                                                                               
                    Record_server.ExecNoQuery( UpdateSQLString )  
                    
        elif ProductionOrRedo == 'Redo':            
            if To == 'DS_Server':#insert into ds server 190
                try:
                    process_insert_structure(df,
                                             DailyQueryTable,
                                             DS_server,
                                             sql_start,
                                             Server,
                                             current,
                                             Type_forRecord, 
                                             StatusTable, 
                                             Record_server,
                                             Destination='DS_Server')                    
                                            
                except BaseException as err:#Fail
                    logging.error('insert gpk table, type = {Type_forRecord}時發生錯誤: {err}'.format(Type_forRecord=Type_forRecord, err=str(err)))
                    sql_process = (time.time()- sql_start)
                    UpdateSQLString = update_string(ProcessTime=sql_process,
                                                    serverip=Server, 
                                                    current=current, 
                                                    Type_forRecord=Type_forRecord,
                                                    Table=StatusTable,
                                                    status='Fail',
                                                    Destination='DS_Server')                                                                                                                                                                               
                    Record_server.ExecNoQuery( UpdateSQLString )  
                 
            elif To == 'Noah1_Server':#insert into ds server 254 
                try:
                    df_noah1 = df[df.SiteId.isin(Noah1_Siteid.Siteid)].reset_index(drop=True)
                    process_insert_structure(df_noah1,
                                             DailyQueryTable,
                                             Noah1_server,                                             
                                             sql_start,
                                             Server,
                                             current,
                                             Type_forRecord, 
                                             StatusTable, 
                                             Record_server,
                                             Destination='Noah1_Server')
                except BaseException as err:#Fail
                    logging.error('insert noah table, type = {Type_forRecord}時發生錯誤: {err}'.format(Type_forRecord=Type_forRecord, err=str(err)))
                    sql_process = (time.time()- sql_start)
                    UpdateSQLString = update_string(ProcessTime=sql_process,
                                                    serverip=Server, 
                                                    current=current, 
                                                    Type_forRecord=Type_forRecord,
                                                    Table=StatusTable,
                                                    status='Fail',
                                                    Destination='Noah1_Server')                                                                                                                                                                               
                    Record_server.ExecNoQuery( UpdateSQLString )                                                    

            elif To == 'Noah2_Server':#insert into noah2 server 170
                try:
                    df_noah2 = df[df.SiteId.isin(Noah2_Siteid.Siteid)].reset_index(drop=True)
                    process_insert_structure(df_noah2,
                                             DailyQueryTable,
                                             Noah2_server,
                                             sql_start,
                                             Server,
                                             current,
                                             Type_forRecord, 
                                             StatusTable, 
                                             Record_server,
                                             Destination='Noah2_Server')
                except BaseException as err:#Fail
                    logging.error('insert noah2 table, type = {Type_forRecord}時發生錯誤: {err}'.format(Type_forRecord=Type_forRecord, err=str(err)))
                    sql_process = (time.time()- sql_start)
                    UpdateSQLString = update_string(ProcessTime=sql_process,
                                                    serverip=Server, 
                                                    current=current, 
                                                    Type_forRecord=Type_forRecord,
                                                    Table=StatusTable,
                                                    status='Fail',
                                                    Destination='Noah2_Server')                                                                                                                                                                               
                    Record_server.ExecNoQuery( UpdateSQLString ) 

    
    for i in range(SQLQuery_df.shape[0]):
        
        sqlquery = SQLQuery_df['Sqlquery'][i]  
        Type_forRecord = SQLQuery_df['Type'][i]
        sql_start = time.time()
        
        if DataNotReady[DataNotReady.Type.isin([Type_forRecord])].empty:
            #Query 
            try:
                
                df = process_query(Section,
                                   DS_server,
                                   Noah1_server,#Noah1_server or Noah2_will run ok.
                                   sqlquery,
                                   Fliter_site) 
            
            except BaseException as err:#Fail
                
                print('Fail')
                logging.error('撈取query table, type = {Type_forRecord}時發生錯誤: {err}'.format(Type_forRecord=Type_forRecord, err=str(err)))
                sql_process = (time.time()- sql_start)
                UpdateSQLString = update_string(ProcessTime=sql_process,
                                                serverip=Server, 
                                                current=current, 
                                                Type_forRecord=Type_forRecord,
                                                Table=StatusTable,
                                                status='Fail',
                                                Destination='all')                                                                                                                                                                               
                Record_server.ExecNoQuery( UpdateSQLString )                           
                continue

            #Insert
            process_insert(df=df,
                           DailyQueryTable=DailyQueryTable,
                           Section=Section,
                           DS_server=DS_server,
                           Noah1_server=Noah1_server,
                           Noah2_server=Noah2_server,
                           sql_start=sql_start,
                           Server=Server,
                           current=current,
                           StatusTable=StatusTable,
                           Type_forRecord=Type_forRecord,
                           Record_server=Record_server,
                           Noah1_Siteid=Noah1_Siteid,
                           Noah2_Siteid=Noah2_Siteid,
                           ProductionOrRedo=ProductionOrRedo,
                           To = To)   
                    
        else:  #in data not ready
            sql_process = (time.time()- sql_start)
            UpdateSQLString = update_string(ProcessTime=sql_process,
                                            serverip=Server, 
                                            current=current, 
                                            Type_forRecord=Type_forRecord,
                                            Table=StatusTable,
                                            status='Fail',
                                            Destination='all')                                                                                                                                                                                                                
            Record_server.ExecNoQuery( UpdateSQLString )                  
