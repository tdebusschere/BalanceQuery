import os
os.chdir('C://Users//DS.Jimmy//Desktop//Project//BalanceCenterHourlyQuery//LagDATA')
import sys
sys.path.append("..//..//")
import Python_global_function.Connect_MSSQL.Connect_SQL as Connect_SQL
import Python_global_function.SendMail.SendMail as SendMail
import under_threshold.Parameter_UnderThreshold as Parameter
import DailyqueryLAGDATA_function as DailyqueryLAGDATA_function
import datetime

##variable zone
#Time and sql connection setting 
now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:00:00.000") 
current = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00.000") 




IP190, JG = Connect_SQL.BalanceCenter_190(), Connect_SQL.JG()

#target rawdatatype
target_type = Parameter.get_TA_type(IP190)


##Processing
#record for this lagdata hour status 
DailyqueryLAGDATA_function.record_lagdata_Hourlystatus(current, 
                                                       JG, 
                                                       status='Insert_Updating')

#get hourly alltype structure
df = DailyqueryLAGDATA_function.get_hourly_alltype_structure(Connect=IP190, 
                                                             now_tw_time=current,
                                                             TA_type=target_type)

##not in TA
DailyqueryLAGDATA_function.insert_not_inTAType(structute=df, 
                                               TA_type=target_type,
                                               Connect=JG)
##in TA
DailyqueryLAGDATA_function.insert_inTAType(structute=df, 
                                           TA_type=target_type, 
                                           Connect=JG, 
                                           utc=now)


#done and update the status table to success
DailyqueryLAGDATA_function.record_lagdata_Hourlystatus(current, 
                                                       JG, 
                                                       status='Update_to_Success')




#mail to aware that.
execute = SendMail.Email( Parameter.Gmail_account )
To_string = SendMail.get_email_to_string(JG, condition='BalanceCenter_DSplusDBA')
DailyqueryLAGDATA_function.condition_happen_email(email_class=execute, 
                                                  To_string=To_string,
                                                  TA_type=target_type,
                                                  Connect=JG, 
                                                  Threshold_times=3)




