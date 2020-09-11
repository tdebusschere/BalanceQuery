import os

import sys
sys.path.append("..//")
import Python_global_function.Connect_MSSQL.Connect_SQL as Connect_SQL
import Python_global_function.SendMail.SendMail_v2 as SendMail
import DS_GetBalanceCenterHourly_parameter_v6 as parameter #String Function & parameters
import Judge_lookuptable_status_v6 as Step1
import Judge_Timezone_function_v6 as Step2
import Process_function_v6 as func
import pandas as pd
import datetime
import argparse



def main(now, current):
    parser = argparse.ArgumentParser(description="python DS_SQLGetDF_V6_schedule.py --server=<serverid>")
    parser.add_argument('--server',nargs=1,type=str)
    try:        
        args = parser.parse_args()  
        
    except:
        sys.stdout.write("python DS_SQLGetDF_V6_schedule.py --server=<serverid>")
        sys.exit()
        
    Server = int(args.server[0])
    if Server not in [191, 192, 193, 194, 195, 196, 197, 198]:
        sys.exit()
        
    '''
    main parameter
    '''
    #SQL Connection & Email class
    ##Read
    Redo_ornot = 'ThisBatch'
    print(Redo_ornot+ '_'+str(Server))
    JG, BalanceCenter_DS = Connect_SQL.JG(), Connect_SQL.BalanceCenter_DS()
    BalanceCenter_Noah1, BalanceCenter_Noah2 =  Connect_SQL.BalanceCenter_Noah1(),  Connect_SQL.BalanceCenter_Noah2()
    Noah1_Siteid, Noah2_Siteid = func.Get_Noah_Site(BalanceCenter_DS)
    
    
    ##Assign   
    serverip, linkserver, ConnectSQL_Query, ConnectSQL_RecordStatus, Section = parameter.Get_IP_LinkSever(Server, 
                                                                                                          BalanceCenter_DS,
                                                                                                          BalanceCenter_Noah1,
                                                                                                          JG)
    #email alert
    execute = SendMail.Email()
    To_string = SendMail.get_email_to_string(JG, 'BalanceCenter_DS')
    
    #for Noah1& Noah2  fliter Siteid. Added on 2020/06/15 by Jimmy Yin.
    Fliter_site = func.Get_Fliter_Site(BalanceCenter_DS, parameter.Exclude_Site)#['NotExcludeNoah1Noah2', 'ExcludeNoah1', 'ExcludeNoah1Noah2']
    
    '''
    Process
    '''
    #step 1: Judge the 190 [BalanceOutcome].[dbo].[LookUpTable] updating or not and the value of the lookuptable.
    #use [BalanceOutcome].[dbo].[DS_LookUptableStatus_STATUS] to judge updateing or not.
    RaworBackup = Step1.Judge_lookuptable_status(parameter.LookUptable_statustable,
                                                 current,
                                                 ConnectSQL_Query,
                                                 sleep_sec=3,
                                                 last_sec=300)
    
    #check the value of the lookuptable.
    correct_df = Step1.Judge_lookuptable_correct(current, 
                                                 parameter.Balance190_lookuptablename_new, 
                                                 ConnectSQL_Query,
                                                 linkserver)            

    print(RaworBackup)
    if RaworBackup != 'Production':
        if Redo_ornot == 'ThisBatch':
            
            Subject, Body = parameter.Email_Lookuptable(current)
            execute.SendMessage(Subject, 
                                Body,
                                To_string)
            
            func.Insert_Status_Fail_FromSuccessMax(ConnectSQL_RecordStatus,
                                                   current,
                                                   serverip, 
                                                   parameter.StatsTable_bytype)    
            sys.exit()
            
        elif Redo_ornot == 'Redo':
            
            sys.exit()
            
    if not correct_df.empty:
        if Redo_ornot == 'ThisBatch':
        
            Subject_correct, Body_correct = parameter.Email_Lookuptable_correct(current)
            execute.SendMessage(Subject_correct, 
                                Body_correct,
                                To_string)
            
            func.Insert_Status_Fail_FromSuccessMax(ConnectSQL_RecordStatus,
                                                   current,
                                                   serverip, 
                                                   parameter.StatsTable_bytype)
            sys.exit()    
            
        else: 
            
            sys.exit() 
            
    #step 2: Judge timezone ready or not(compare the lookuptable in 190 with timezone value in JG)
    Step2.Judge_timezone_status(now,
                                current,
                                Server, 
                                ConnectSQL_Query, 
                                parameter.Balance190_lookuptablename_new,
                                parameter.lookuptable_col,
                                linkserver,
                                ConnectSQL_RecordStatus, 
                                parameter.JG_TimezoneTable)
    
    #Step3:Judge LAGData Procedure (added by jimmyyin on 2020/02/05)   
    step3_result = func.JudgeLAGDataProcedure(parameter.JG_LagDataTable_status,
                                              current,
                                              ConnectSQL_RecordStatus,
                                              sleep_sec=3, 
                                              last_sec=600)
    print(step3_result)
    if step3_result == "LAGData Procedure isn't done yet":
        if Redo_ornot == 'ThisBatch':
        
            func.Insert_Status_Fail_FromSuccessMax(ConnectSQL_RecordStatus,
                                                   current,
                                                   serverip, 
                                                   parameter.StatsTable_bytype)        
            lagdata_Subject, lagdata_Body = parameter.Email_LAGDataProcedure(current)
            execute.SendMessage(lagdata_Subject, 
                                lagdata_Body, 
                                To_string)
            sys.exit()
            
        elif Redo_ornot == 'Redo':            
            sys.exit()   
            
    else:
        
        DataNotReady_string = "select *\
                               from {JG_LagDataTable}\
                               where [LinkServerName] = '{LinkServer}' \
                                     and [DatePlayed] = '{current}' \
                                     and status = 'Wait'".format(JG_LagDataTable=parameter.JG_LagDataTable,    
                                                                 LinkServer=linkserver,
                                                                 current=current)
        DataNotReady =  ConnectSQL_RecordStatus.ExecQuery(DataNotReady_string) 

    
    '''    
    #step 4: Gather the status = 'Fail' and lag data procedure status = ['Success', 'ReRun']  before this batch  and redo.
    redo_str = "SELECT statustable.* \
                FROM {StatsTable_bytype} statustable\
                LEFT JOIN {serverlookup} serverlookup\
                ON statustable.SERVER = serverlookup.SERVER\
                INNER JOIN {JG_LagDataTable} lagdata\
                ON statustable.type = lagdata.type \
                AND statustable.updatetime = lagdata.dateplayed \
                AND serverlookup.[LinkServerName] = lagdata.[LinkServerName] \
                WHERE  statustable.status = 'Fail' \
                       AND statustable.server = '{serverip}'\
                	   AND lagdata.status IN ( 'Success', 'ReRun' )".format(StatsTable_bytype=parameter.StatsTable_bytype,
                                                                            serverlookup=parameter.server_lookup_table,
                                                                             serverip=serverip,
                                                                             JG_LagDataTable=parameter.JG_LagDataTable)
                     
    redo_df = ConnectSQL_RecordStatus.ExecQuery( redo_str )
    
    if redo_df.empty:
        
        print("Didn't need to redo")
        
    else:
        
        print('Need to redo')
        #change status to running
        redo_df.columns = parameter.StatsTable_bytype_col
        update_running =  "UPDATE {StatsTable_bytype} \
                           SET Status = 'Running',  Exe_Time_sec = NULL \
                           where {WHERE_CONDITION}".format(StatsTable_bytype=parameter.StatsTable_bytype,
                                                           WHERE_CONDITION=func.String_OR(redo_df))
        ConnectSQL_RecordStatus.ExecNoQuery(update_running)        
        print('updating status to running is done')
        
        #redo
        for r in range(redo_df.shape[0]):
            Type = redo_df.Type[r]
            To = redo_df.Destination[r]
            #time
            tw = redo_df.UpDateTime[r]
            d = datetime.timedelta(hours = -8)
            UpDateTime_utc = (tw+ d).strftime("%Y-%m-%d %H:00:00.000")
            UpDateTime_tw = redo_df.UpDateTime[r].strftime("%Y-%m-%d %H:00:00.000")
            #print(Type, UpDateTime_tw, UpDateTime_utc)

            
            SQLQuery_redo_df = func.select_from_sql(UpDateTime_utc, 
                                                    UpDateTime_tw, 
                                                    linkserver, 
                                                    ConnectSQL_Query, 
                                                    JG)
            SQLQuery_redo_df = SQLQuery_redo_df[(SQLQuery_redo_df.Type == Type)].reset_index(drop=True)
            
            #print(SQLQuery_redo_df.shape)
            #Redo start
            func.process(ProcessDf=SQLQuery_redo_df,
                         Connect_DSServer=BalanceCenter_DS,
                         Connect_Noah1Server=BalanceCenter_Noah1,
                         Connect_Noah2Server=BalanceCenter_Noah2,      
                         Connect_RecordServer=ConnectSQL_RecordStatus,
                         Server=serverip,
                         Time=UpDateTime_tw,
                         DailyQueryTable=parameter.DailyQueryTable_all,
                         LagData=DataNotReady,
                         Fliter=Fliter_site,
                         StatusTable=parameter.StatsTable_bytype,
                         Section=Section,
                         Noah1_Siteid=Noah1_Siteid,
                         Noah2_Siteid=Noah2_Siteid,
                         ProductionOrRedo = 'Redo',#Redo
                         To = To#['DS_Server', 'Noah1_Server', 'Noah2_Server']
                        )  

        
            ConnectSQL_RecordStatus.ExecNoQuery("UPDATE {JG_LagDataTable}  \
                                                SET Status = 'Success'\
                                                where type = {Type} \
                                                      AND dateplayed = '{UpDateTime_tw}'\
                                                      AND [LinkServerName] = '{linkserver}'".format(JG_LagDataTable=parameter.JG_LagDataTable,
                                                                                                        Type=Type,
                                                                                                        UpDateTime_tw=UpDateTime_tw,
                                                                                                 linkserver=linkserver))
     '''  
    #step 5: process this batch
    #make lookuptable group by type first
    SQLQuery_df = func.select_from_sql(now,
                                       current,
                                       linkserver,
                                       ConnectSQL_Query,
                                       JG)
    
    #Record by type    
    func.Record_By_Type(ProcessDf=SQLQuery_df,
                        Server=serverip,                     
                        Time=current, 
                        Connect_RecordServer=ConnectSQL_RecordStatus, 
                        StatusTable=parameter.StatsTable_bytype,
                        Section=Section)
        
    #process this batch   
    func.process(ProcessDf=SQLQuery_df,
                 Connect_DSServer=BalanceCenter_DS,
                 Connect_Noah1Server=BalanceCenter_Noah1,
                 Connect_Noah2Server=BalanceCenter_Noah2,      
                 Connect_RecordServer=ConnectSQL_RecordStatus,
                 Server=serverip,
                 Time=current,
                 DailyQueryTable=parameter.DailyQueryTable_all,
                 LagData=DataNotReady,
                 Fliter=Fliter_site,
                 StatusTable=parameter.StatsTable_bytype,
                 Section=Section,
                 Noah1_Siteid=Noah1_Siteid,
                 Noah2_Siteid=Noah2_Siteid,
                 ProductionOrRedo = 'Production',#Redo
                 To = 'DS_Server')#Meaningless
                 
    return 1

#time
now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:00:00.000") #UTC+0
current = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00.000") #UTC+8
#now = '2020-07-14 00:00:00.000'
#current = '2020-07-14 08:00:00.000'
xmm = main(now, current)
