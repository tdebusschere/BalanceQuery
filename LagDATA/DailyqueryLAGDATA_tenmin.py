import os
os.chdir('C://Users//DS.Jimmy//Desktop//Project//BalanceCenterHourlyQuery//LagDATA')
import sys
sys.path.append("..//..//")
import Python_global_function.Connect_MSSQL.Connect_SQL as Connect_SQL
import DailyqueryLAGDATA_function as DailyqueryLAGDATA_function


JG = Connect_SQL.JG()
rerun_df = JG.ExecQuery("SELECT * \
                        FROM DataScientist.dbo.DailyqueryLAGDATASTATUS \
                        WHERE STATUS = 'Wait' ")

DailyqueryLAGDATA_function.update_Tablemax_and_Status(rerun_df, JG)



