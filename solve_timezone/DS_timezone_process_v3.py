import sys
import argparse
import os
os.chdir('C://Users//DS.Jimmy//Desktop//Project//solve_timezone')
sys.path.append("..//")
import Python_global_function.Connect_MSSQL.Connect_SQL as Connect_SQL
import Python_global_function.SendMail.SendMail as SendMail
import time
import datetime
import DS_timezone_function_v3 as func
from dateutil.relativedelta import relativedelta

def main():
    parser = argparse.ArgumentParser(description="python DS_Timezone_v3.py --server=<serverid>")
    parser.add_argument('--server',nargs=1,type=str)
    try:
        args = parser.parse_args()
  
    except:
        sys.stdout.write("python DS_Timezone_v3.py -server=<serverid>")
        sys.exit()
        
    Server = int(args.server[0])
    if Server not in [191, 192, 193, 194, 195, 196, 197, 198]:
        sys.exit()

    #s = time.time()
    #Server_list = [191, 192, 193, 194]
    #Server = Server_list[2]
    #IP = func.Duizhang(Server)
    '''
    JG = func.JG()
    IP = func.BalanceCenter_190()
    linkserver = "Balance{}R".format(Server)    
    '''
    
    
    JG, BalanceCenter_DS = Connect_SQL.JG(), Connect_SQL.BalanceCenter_DS()
    BalanceCenter_Noah1, BalanceCenter_Noah2 =  Connect_SQL.BalanceCenter_Noah1(),  Connect_SQL.BalanceCenter_Noah2()
    Noah1_Siteid, Noah2_Siteid = func.Get_Noah_Site(BalanceCenter_DS)
    
    ##Assign   
    serverip, linkserver, ConnectSQL_Query, ConnectSQL_RecordStatus, Section = func.Get_IP_LinkSever(Server, 
                                                                                                     BalanceCenter_DS,
                                                                                                     BalanceCenter_Noah1,
                                                                                                     JG)
    if Section == 'GPK':
        IP = BalanceCenter_DS
    else:
        IP = BalanceCenter_Noah1
        
    
    #main parameter
    current = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.000") 
    result_df = func.select_from_sql(linkserver, 
                                     IP,
                                     BalanceCenter_DS, 
                                     JG,
                                     Section)

    
    result_df.loc[:, 'Batch_time'] = current
    result_df.loc[:, 'Server'] = serverip
   
    result_df = result_df[['Batch_time', 'Server', 'Type', 'table_max', 'utc0', 'diff_min']].reset_index(drop=True)
        
    result_df.loc[result_df.table_max.isna(),'table_max'] = 0
    result_df.loc[result_df.diff_min.isna(),'diff_min'] = 0
    
        
    BalanceCenter_DS.Executemany("insert into DataScientist.dbo.DS_TimezoneValue\
                                  ([Batch_time], [Server], [Type], [table_max], [utc0], [diff_min])\
                                  values (?,?,?,?,?,?)", result_df)
    update_string = "update  DataScientist.dbo.DS_TimezoneValue\
                     set table_max = NULL, diff_min = NULL \
                     WHERE table_max =0"
    BalanceCenter_DS.ExecNoQuery( update_string )



#do
main()
