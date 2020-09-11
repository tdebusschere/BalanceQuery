import pandas as pd
'''
Function Zone
'''
def Judge_timezone_status(now, current, Server, BalanceCenter_190, Balance190_lookuptablename, lookuptable_col, linkserver, ConnectSQL_RecordStatus, JG_TimezoneTable):

    def TimezoneReadyOrNot(BalanceCenter_190, Balance190_lookuptablename, linkserver, ConnectSQL_RecordStatus, JG_TimezoneTable):
        
        sql_lookuptable = "SELECT distinct [Type]\
                           FROM {table} \
                           where [LinkServerName ] = '{linkserver}'".format(table=Balance190_lookuptablename,
                                                                             linkserver=linkserver)
        lookuptable = BalanceCenter_190.ExecQuery(sql_lookuptable)
        
        JG_Timezone = ConnectSQL_RecordStatus.ExecQuery("SELECT Type  FROM {JG_TimezoneTable}".format(JG_TimezoneTable=JG_TimezoneTable))
        Tmezone_notready_type = lookuptable[~lookuptable.Type.isin(JG_Timezone.Type)].reset_index(drop=True)
        
        return Tmezone_notready_type

    def get_timezone_frominfo(now, BalanceCenter_190, fliter, current, Balance190_lookuptablename,lookuptable_col):
        def get_target_old(BalanceCenter_190, Balance190_lookuptablename, lookuptable_col, current):
            t = str(current[2:4]+ current[5:7])
            chart = BalanceCenter_190.ExecQuery("select * from {table} where \
                                                [MonthDB] = '{day}' or [MonthDB] is null".format(day=t,
                                                                                                 table=Balance190_lookuptablename))
            chart.columns = lookuptable_col   
            target = chart.copy()
            
            
    
            target['DBName_sql'] = target.apply(lambda row: 
                str(row['DBName'])
                + now[2:4]
                + now[5:7] if row['MonthDB'] is not None else row['DBName'], axis=1)
            
            #group by type
            target_GroupByType = target[['LinkServerName', 'Type', 'DBName_sql', 'TableName']].drop_duplicates()
            target_GroupByType = target_GroupByType.reset_index(drop=True)
            
            return target_GroupByType
            
        def SQL_timezonenew_sqlstring_old(row):
            server = row['LinkServerName']
            Type = row['Type']
            DBName = row['DBName_sql']
    
            return "SELECT Type, [Timezone] \
                    FROM {server}.{DBName}.[dbo].[VW_RawDataInfo] where type = {Type}".format(server=server, 
                                                                                              DBName=DBName,
                                                                                              Type=Type)
      
        target_GroupByType = get_target_old(BalanceCenter_190, Balance190_lookuptablename, lookuptable_col, current) 
        
        target_GroupByType.loc[:, 'timezonenew_sqlstring'] = target_GroupByType.apply(lambda row: SQL_timezonenew_sqlstring_old(row), axis=1)
        
        result = pd.DataFrame(columns=['Type', 'Timezone'])
        for i in range(target_GroupByType.shape[0]):
            sqlstring = target_GroupByType.timezonenew_sqlstring[i]
            temp = BalanceCenter_190.ExecQuery(sqlstring)
            result = pd.concat([result, temp], axis = 0)
            
        result = result[result.Type.isin(fliter)]
        result = result.drop_duplicates().reset_index(drop=True)
        result.loc[:,'updatetime'] = current ; result.loc[:,'status'] = 'Success'
                
        return result    
    
    Tmezone_notready_type = TimezoneReadyOrNot(BalanceCenter_190,
                                               Balance190_lookuptablename,
                                               linkserver, 
                                               ConnectSQL_RecordStatus, 
                                               JG_TimezoneTable)

    if Tmezone_notready_type.empty:
        print("Tmezone is ready in {}".format(Server))
    else:
        print("Tmezone is not ready in {}".format(Server))
        insert_timezone_df = get_timezone_frominfo(now,
                                                   BalanceCenter_190,
                                                   Tmezone_notready_type.Type,
                                                   current,
                                                   Balance190_lookuptablename,
                                                   lookuptable_col)
        ConnectSQL_RecordStatus.Executemany("insert into {table} ([Type], [Timezone], [UpdateTime], [status]) \
                        values (?,?,?,?)".format(table=JG_TimezoneTable), insert_timezone_df)