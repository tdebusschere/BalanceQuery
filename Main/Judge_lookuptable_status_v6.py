import pandas as pd
import logging
import time
logging.basicConfig(filename= 'run.log', level = logging.ERROR) #loggin設定
pd.set_option('display.max_columns', None)


'''
Function Zone
'''
#step 1: Judge the 190 [BalanceOutcome].[dbo].[LookUpTable] updating or not.
#use [BalanceOutcome].[dbo].[DS_LookUptableStatus_STATUS] to judge
def Judge_lookuptable_status(LookUptable_statustable, current, ConnectSQL_Query, sleep_sec=3, last_sec=600):
    last = int(last_sec/sleep_sec)
    for k in range(0, last):
        #print(k)
        sqlquery = "SELECT * \
                    FROM {table} \
                    where [LookUpNewUpdateTime] >= '{current}' \
                          and [LookUpNewStatus] = 1".format(table=LookUptable_statustable,
                                                            current=current)
                    
        df = ConnectSQL_Query.ExecQuery(sqlquery)
        if not df.empty:
            result = 'Production'
            return result
            break
        elif((df.empty) & (k == (last -1))):
            result = 'JG'
            return result
        else:
            time.sleep(sleep_sec)#stop 3sec, last to 10 min
            

def Judge_lookuptable_correct(current, LookUptable, ConnectSQL_Query, linkserver):
    Month = current[2:4]+ current[5:7]
    query = " SELECT [LinkServerName ]\
              ,[DBName]\
              ,[MonthDB]\
              ,[TableName]\
        	  ,[Type]\
          FROM {LookUptable}\
          where [GameTypeSourceId] in (\
        	  SELECT [GameTypeSourceId]\
        	  FROM {LookUptable}\
        	  where [MonthDB] = '{Month}' or [MonthDB] is null\
        	  group by  [GameTypeSourceId]\
        	  having count(1) >= 2 )\
        group by [LinkServerName ]\
              ,[DBName]\
              ,[MonthDB]\
              ,[TableName]\
        	  ,[Type]\
        order by [Type]\
        		, [LinkServerName ]\
        		, [DBName]".format(LookUptable=LookUptable,
                                   Month=Month)  
          
    correct_df = ConnectSQL_Query.ExecQuery(query)    
    correct_df.columns = ['LinkServerName', 'DBName', 'MonthDB', 'TableName', 'Type']
    
    correct_df = correct_df[correct_df.LinkServerName == linkserver]
    
    return correct_df
          