import os
os.chdir('C://Users//DS.Jimmy//Desktop//Project//solve_timezone')
import sys
sys.path.append("..//")
import Python_global_function.Connect_MSSQL.Connect_SQL as Connect_SQL
import Python_global_function.SendMail.SendMail as SendMail
import datetime
import time
import pandas as pd
import DS_timezone_function_v3 as func
import DS_Email as DS_Email
pd.set_option("display.max_rows", 1000)
pd.set_option("display.max_columns", 1000)

##variable zone
#time
s = time.time()
now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.000") 
current = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.000") 
#connect SQL
JG, BalanceCenter_DS = Connect_SQL.JG(), Connect_SQL.BalanceCenter_DS()
BalanceCenter_Noah1, BalanceCenter_Noah2 =  Connect_SQL.BalanceCenter_Noah1(),  Connect_SQL.BalanceCenter_Noah2()


##update the status table
'''
Source = JG.ExecQuery("select [Batch_time]\
                    ,'GPK' [path]\
            		,[Server]\
            		,[Type]\
            		,[table_max]\
            		,[utc0]\
            		,[diff_min]  \
                    FROM [DataScientist].[dbo].[DS_TimezoneValue]\
            where server in ('10.80.16.191', '10.80.16.192',\
                            '10.80.16.193', '10.80.16.194', \
                            '10.80.16.195')\
            union all\
            SELECT [Batch_time]\
                   , 'Noah' [path]\
                  ,[Server]\
                  ,[Type]\
                  ,[table_max]\
                  ,[utc0]\
                  ,[diff_min]\
             FROM [DataScientist].[dbo].[DS_TimezoneValue]\
            where server in ('10.80.26.249', '10.80.26.250', '10.80.26.253')\
            and [Type] not in (select type  FROM [DataScientist].[dbo].[DS_TimezoneValue]\
            				where server in ('10.80.16.191', '10.80.16.192',\
                                                '10.80.16.193', '10.80.16.194', \
                                                '10.80.16.195'))")
'''

##GPK
Source = BalanceCenter_DS.ExecQuery("select [Batch_time]\
                                    ,'GPK' [path]\
                            		,[Server]\
                            		,[Type]\
                            		,[table_max]\
                            		,[utc0]\
                            		,[diff_min]  \
                                    FROM [DataScientist].[dbo].[DS_TimezoneValue]\
                            where server in ('10.80.16.191', '10.80.16.192',\
                                            '10.80.16.193', '10.80.16.194', \
                                            '10.80.16.195')")
timezone_result = func.Get_Timezone_DS_Result(Source)
old_timezone_result = func.get_old_timezone(now, BalanceCenter_DS)
type_category = func.Get_CateGory(BalanceCenter_DS)
timezone_result_final = func.Merge_and_GetTimezone(timezone_result,
                                                   old_timezone_result,
                                                   type_category)
timezone_result_final = timezone_result_final[['Type', 'Timezone']].reset_index(drop=True)
timezone_result_final['Timezone'] = timezone_result_final['Timezone'].astype('int64')



#NOAH
Source2 = BalanceCenter_DS.ExecQuery("SELECT [Batch_time]\
                                           , 'Noah' [path]\
                                          ,[Server]\
                                          ,[Type]\
                                          ,[table_max]\
                                          ,[utc0]\
                                          ,[diff_min]\
                                     FROM [DataScientist].[dbo].[DS_TimezoneValue]\
                                    where server in ('10.80.26.249', '10.80.26.250', '10.80.26.253')\
                                    and [Type] not in (select type  FROM [DataScientist].[dbo].[DS_TimezoneValue]\
                                    				where server in ('10.80.16.191', '10.80.16.192',\
                                                                        '10.80.16.193', '10.80.16.194', \
                                                                        '10.80.16.195'))")
timezone_result2 = func.Get_Timezone_DS_Result(Source2)
old_timezone_result2 = func.get_old_timezone(now, BalanceCenter_Noah1)
type_category = func.Get_CateGory(BalanceCenter_DS)
timezone_result_final2 = func.Merge_and_GetTimezone(timezone_result2,
                                                   old_timezone_result2,
                                                   type_category)
timezone_result_final2 = timezone_result_final2[['Type', 'Timezone']].reset_index(drop=True)
timezone_result_final2['Timezone'] = timezone_result_final2['Timezone'].astype('int64')


timezone_result_final = pd.concat([timezone_result_final, timezone_result_final2], axis=0)
timezone_result_final = timezone_result_final.sort_values(by=['Type'],  ascending=[True]).reset_index(drop=True)



raw = BalanceCenter_DS.ExecQuery("    select Type, Timezone  as Timezone_old from DataScientist.[dbo].[DS_DuizhangTimeZone] ")
timezone_result_final_inraw = timezone_result_final[timezone_result_final.Type.isin(raw.Type)].reset_index(drop=True)
timezone_result_final_inraw = timezone_result_final_inraw.merge(raw,
                                                                how='left',
                                                                on='Type')





#END
judge_changeornot = timezone_result_final_inraw[timezone_result_final_inraw.Timezone != timezone_result_final_inraw.Timezone_old]
if judge_changeornot.empty:
    timezone_result_final_notinraw = timezone_result_final[~timezone_result_final.Type.isin(raw.Type)].reset_index(drop=True)
    
        
    BalanceCenter_DS.ExecNoQuery("UPDATE DataScientist.[dbo].[DS_DuizhangTimeZone] \
                                  SET [Status] = 'Updating'")
    
    if timezone_result_final_notinraw.empty:
        print(1)
    else:
        timezone_result_final_notinraw.loc[:, 'UpdateTime'] = current
        BalanceCenter_DS.Executemany("insert into [DataScientist].[dbo].[DS_DuizhangTimeZone]\
                                      ([Type], [Timezone], [UpdateTime]) \
                                      values (?,?,?)", timezone_result_final_notinraw)
    
    
    for j in range(timezone_result_final.shape[0]):
        Type = timezone_result_final.Type[j]
        TimezoneValue = int(timezone_result_final.Timezone[j])
      
        UpdateString = "UPDATE DataScientist.[dbo].[DS_DuizhangTimeZone]\
                        SET [Status] = 'Success',Timezone = {TimezoneValue}, UpdateTime = '{updatetime}'\
                         where Type = {Type} ".\
                        format(TimezoneValue=TimezoneValue, updatetime=current,Type= Type)
        BalanceCenter_DS.ExecNoQuery(UpdateString)
        #print(j)    
else:
    execute = DS_Email.Email()
    Subject, Body = "Timezone is changed!!", "Timezone is changed!  Jimmy didn't update it on {current} and tell him check it out then manually update it".format(current=current)
    execute.SendMessage(Subject, Body)

'''
for j in range(old_timezone_result.shape[0]):
    Type = old_timezone_result.Type[j]
    TimezoneValue = int(old_timezone_result.Timezone_DBA[j])
  
    UpdateString = "UPDATE DataScientist.[dbo].[DS_DuizhangTimeZone]\
                    SET [Status] = 'Success',Timezone = {TimezoneValue}, UpdateTime = '{updatetime}'\
                     where Type = {Type} ".\
                    format(TimezoneValue=TimezoneValue, updatetime=current,Type= Type)
    JG.ExecNoQuery(UpdateString)
    #print(j)
'''










