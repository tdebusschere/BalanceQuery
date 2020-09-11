import pyodbc
import pandas as pd
import numpy as np
import keyring
import sys
import logging
from dateutil.relativedelta import relativedelta


'''
Function Zone
'''
def Get_IP_LinkSever(Server, BalanceCenter_DS, BalanceCenter_Noah, JG):
    
    if Server in [191, 192, 193, 194, 195]:
        
        serverip = "10.80.16.{}".format(Server)
        linkserver = "Balance{}R".format(Server)   
        ConnectSQL_Query = BalanceCenter_DS
        Section = 'GPK'
        
    elif Server == 196:
        
        serverip = "10.80.26.249"
        linkserver = "BalanceN191R"
        ConnectSQL_Query = BalanceCenter_Noah
        Section = 'Noah'
        
    elif Server == 197:

        serverip = "10.80.26.250"
        linkserver = "BalanceN192R"
        ConnectSQL_Query = BalanceCenter_Noah
        Section = 'Noah'

    elif Server == 198:
        
        serverip = "10.80.26.253"
        linkserver = "BalanceN193R"         
        ConnectSQL_Query = BalanceCenter_Noah
        Section = 'Noah'
   
    ConnectSQL_RecordStatus = JG 
    
    return serverip, linkserver, ConnectSQL_Query, ConnectSQL_RecordStatus, Section


def Get_Noah_Site(BalanceCenter_190):
    Noah1_query = "SELECT [Siteid]\
                   FROM [DataScientist].[dbo].[DS_BalanceCenterDailyQuery_NoahSite]\
                   WHERE [SiteType] = 'Noah1'" 

    Noah2_query = "SELECT [Siteid]\
                   FROM [DataScientist].[dbo].[DS_BalanceCenterDailyQuery_NoahSite]\
                   WHERE [SiteType] = 'Noah2'" 
    
    Noah1_Siteid, Noah2_Siteid = BalanceCenter_190.ExecQuery(Noah1_query), BalanceCenter_190.ExecQuery(Noah2_query)
    
    return Noah1_Siteid, Noah2_Siteid


'''
result_df = func.select_from_sql(linkserver, 
                                 IP,
                                 BalanceCenter_DS, 
                                 JG,
                                 Section)
'''


def select_from_sql(linkserver, IP, BalanceCenter_DS, JG, Section):
    def get_target():
        try:
            if Section == 'GPK':
                chart = IP.ExecQuery("select * from [BalanceOutcome].[dbo].[LookUpTableNew]")
            elif Section == 'Noah':
                chart = IP.ExecQuery("select * from [BalanceOutcome].[dbo].[LookUpTableNew]")
                
        except:
            chart = JG.ExecQuery("select * from [BalanceOutcome].[dbo].[LookUpTable]")

        try:
            Exclude_rawdatatype_df = BalanceCenter_DS.ExecQuery("SELECT distinct [RawDataType] Type\
                                                       FROM [DataPool].[dbo].[VW_GameLookup]\
                                                       where [GameHallName] in ('SE','3Sing','TGP2','PS2')")
            Exclude_rawdatatype = list(Exclude_rawdatatype_df.Type)
        except:
            Exclude_rawdatatype = [35, 67, 68, 139]# Mako said that there aren't unique gameaccount in these gamehall(rawdatatype)
        
        chart.columns = ['LinkServerName', 'GameTypeSourceId', 'DBName', 'MonthDB', 'TableName', 'Type']    
        

        #fliter
        target = chart[~chart['GameTypeSourceId'].isin(Exclude_rawdatatype)].copy()
        target = target[target['LinkServerName'] == linkserver]
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

    
    def SQL_max_sqlstring(row, target):
        
        linkserver = row['LinkServerName']
        Table = row['TableName']
        MonthDB = row['MonthDB']
        type = row['Type']
        if MonthDB is None:            
            DBName = str(row['DBName'])
        else:
            DBName = str(row['DBName']) + str(row['MonthDB'])
            

        if DBName == '_CasinoBalanceCenter_BalanceC':
            if target[target.Type == type].GameTypeSourceId.shape[0] == 1:
                type_game = "("+ str(target[target.Type == type].GameTypeSourceId.iloc[0]) + ")"
            else:
                type_game = tuple(target[target.Type == type].GameTypeSourceId)
                type_game = str(type_game)
                
            return "SELECT max(wagerstime) table_max,\
                           GETUTCDATE() utc0,\
                           datediff(minute,max(wagerstime),\
                           GETUTCDATE()) diff_min \
                    from {linkserver}.{DBName}.[dbo].vw_{Table} where GameTypeSourceId in {type_game}".format(linkserver=linkserver,
                                                                                                              DBName=DBName,
                                                                                                              Table=Table, 
                                                                                                              type_game=type_game)
        else:
            return "SELECT max(wagerstime) table_max,\
                           GETUTCDATE() utc0, \
                           datediff(minute,max(wagerstime),\
                           GETUTCDATE()) diff_min \
                    from {linkserver}.{DBName}.[dbo].vw_{Table}".format(linkserver=linkserver,
                                                                        DBName=DBName,
                                                                        Table=Table)
        
    target = get_target()
    groupbytype = group_by_type(target)     
    groupbytype.loc[:, 'SQL_max_sqlstring'] = groupbytype.apply(lambda row: SQL_max_sqlstring(row, target), axis=1)

    result = pd.DataFrame(columns=['table_max', 'utc0', 'diff_min'])
    x=[]
    for i in range(groupbytype.shape[0]):
        sqlstring = groupbytype.SQL_max_sqlstring[i]
        try:
            temp = IP.ExecQuery(sqlstring)
            result = pd.concat([result, temp], axis = 0)
        except:
            x.append(i)
            temp = pd.DataFrame({'table_max':None,
                                 'utc0':None,
                                 'diff_min':None}, index=[0])
            result = pd.concat([result, temp], axis = 0)
    
    result = result.reset_index(drop=True)
    if len(x) != 0:
        max_date = np.max(result[result.index < np.min(x)].utc0)
        result.loc[x,'utc0'] = max_date
     
    result.loc[:, 'Type'] =  groupbytype.Type 
    result = result.sort_values(by=['Type'],  ascending=[True]).reset_index(drop=True)


    result_final = result.sort_values('table_max', ascending=False).groupby('Type', as_index=False).first()
    result_final = result_final[['table_max', 'utc0', 'diff_min', 'Type']]
    return result_final

def Get_Timezone_DS_Result(Source):
    timezone_result_str = pd.DataFrame({'Type':np.unique(Source.Type)})#str
    timezone_result = pd.DataFrame(columns = ['Type', 'diff_min', 'count', 'variance'])


    for j in np.unique(Source.Type):
        temp = Source[Source.Type == j]
        temp = temp.sort_values(by='Batch_time', ascending=True).reset_index(drop=True)
        temp.loc[:, 'table_max_lag'] = temp.table_max.shift(1)
        
        condition_notFirstRow = ( temp.index != 0 )
        condition_notUpdating = ( temp.table_max != temp.table_max_lag )
        ans_df = temp[ condition_notFirstRow & condition_notUpdating]
        ans_df = ans_df.reset_index(drop=True)
        
        #added by jimmy yin on 2020/06/29
        ans_df['table_max'] = ans_df['table_max'].astype('datetime64[ns]')
        ans_df['diff_min'] = ans_df['diff_min'].astype('float64')
        ans_df['table_max_lag'] = ans_df['table_max_lag'].astype('datetime64[ns]')
        
        agg = {'diff_min': ['mean', 'count', 'var']}
        ans_tmp = ans_df[['Type', 'diff_min']].groupby(['Type'], as_index=False).agg(agg)   
        ans_tmp.columns = ['Type', 'diff_min', 'count', 'variance']
        timezone_result = pd.concat([timezone_result, ans_tmp], axis = 0)

    timezone_result = timezone_result.reset_index(drop=True)
    timezone_result = timezone_result_str.merge(timezone_result, 
                                                   how = 'left', 
                                                   on = ['Type'])
    timezone_result.loc[:, 'sth_left'] = timezone_result.diff_min % 60
    timezone_result.loc[:, 'timezone_jy'] = -1 *(timezone_result.diff_min // 60)
    return timezone_result


def get_old_timezone(now, IP):
    def get_target_old(IP):
        chart = IP.ExecQuery("select * from [BalanceOutcome].[dbo].[LookUpTablenew]")
        chart.columns = ['LinkServerName', 'GameTypeSourceId', 'DBName', 'MonthDB', 'TableName', 'Type']    
        target = chart.copy()

        target['DBName_sql'] = target.apply(lambda row: 
            str(row['DBName'])
            + now[2:4]
            + now[5:7] if row['MonthDB'] else row['DBName'], axis=1)
        
        #group by type
        target_GroupByType = target[['LinkServerName', 'Type','DBName_sql', 'TableName']].drop_duplicates()
        target_GroupByType = target_GroupByType.reset_index(drop=True)
        
        return target_GroupByType, target
        
    def SQL_timezonenew_sqlstring_old(row):
        server = row['LinkServerName']
        Type = row['Type']
        DBName = row['DBName_sql']

        return "SELECT Type, [Timezone] \
                FROM {server}.{DBName}.[dbo].[VW_RawDataInfo] where type = {Type}".format(server=server, 
                                                                                          DBName=DBName,
                                                                                          Type=Type)
  
    target_GroupByType, target = get_target_old(IP) 
    
    target_GroupByType.loc[:, 'timezonenew_sqlstring'] = target_GroupByType.apply(lambda row: SQL_timezonenew_sqlstring_old(row), axis=1)
    
    result = pd.DataFrame(columns=['Type', 'Timezone'])
    for i in range(target_GroupByType.shape[0]):
        sqlstring = target_GroupByType.timezonenew_sqlstring[i]
        temp = IP.ExecQuery(sqlstring)
        result = pd.concat([result, temp], axis = 0)
    result = result.reset_index(drop=True)
    result.columns = ['Type', 'Timezone_DBA']    
    result = result.drop_duplicates()
    
    return result



def Get_CateGory(IP):
    query_string = "SELECT * FROM [DataPool].[dbo].[VW_GameLookup]"
    type_category = IP.ExecQuery(query_string)
    type_category = type_category[['RawDataType', 'Category']].drop_duplicates().reset_index(drop=True)
    type_category.columns = ['Type', 'Category']
    return type_category


def Merge_and_GetTimezone(timezone_result, old_timezone_result, type_category):
    def Get_Real_Timezone_value(timezone_result_final):
        timezone_value = []
        for i in range(timezone_result_final.shape[0]):
            condition_nonele = (timezone_result_final.Category[i] != '機率')
            condition_noinformation = (np.isnan(timezone_result_final.timezone_jy[i]))
            if condition_nonele | condition_noinformation:
                temp = timezone_result_final.Timezone_DBA[i]
            else:
                temp = timezone_result_final.timezone_jy[i]
            timezone_value.append(temp)
        return timezone_value
    timezone_result_final = timezone_result.merge(old_timezone_result, 
                                              how = 'left', 
                                              on = ['Type'])
    timezone_result_final = timezone_result_final.merge(type_category, 
                                                        how = 'left', 
                                                        on = ['Type'])
    
    timezone_result_final.loc[:, 'Timezone'] = Get_Real_Timezone_value(timezone_result_final)

    return timezone_result_final

