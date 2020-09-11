import datetime
'''
Function Zone
'''
def Get_IP_LinkSever(Server, BalanceCenter_DS, BalanceCenter_Noah, JG):
    
    if Server in [191, 192, 193, 194, 195]:
        
        serverip = "10.80.16.{}".format(Server)
        linkserver = "Balance{}R".format(Server)   
        ConnectSQL_Query = BalanceCenter_DS
        Section = 'FromGPKBalanceCenterToDSServer'
        
    elif Server == 196:
        
        serverip = "10.80.26.249"
        linkserver = "BalanceN191R"
        ConnectSQL_Query = BalanceCenter_Noah
        Section = 'FromNoahBalanceCenterToDSServerNoahServer'
        
    elif Server == 197:

        serverip = "10.80.26.250"
        linkserver = "BalanceN192R"
        ConnectSQL_Query = BalanceCenter_Noah
        Section = 'FromNoahBalanceCenterToDSServerNoahServer'

    elif Server == 198:
        
        serverip = "10.80.26.253"
        linkserver = "BalanceN193R"         
        ConnectSQL_Query = BalanceCenter_Noah
        Section = 'FromNoahBalanceCenterToDSServerNoahServer'
   
    ConnectSQL_RecordStatus = BalanceCenter_DS 
    
    return serverip, linkserver, ConnectSQL_Query, ConnectSQL_RecordStatus, Section


#parameter.Email_Lookuptable(current)



def Email_Lookuptable(current):    
    #Email parameter
    Email_subject = "Lookuptable is not updating on '{time}', please tell yao to update it".format(time=current)
    Email_body = "Lookuptable is not updating , please tell yao to update it"
    
    return Email_subject, Email_body

def Email_Lookuptable_correct(current):    
    #Email parameter
    Email_subject = "Lookuptable is not correct on '{time}', please tell yao to aware it".format(time=current)
    Email_body = "Lookuptable is not correct , please tell yao to aware it"
    
    return Email_subject, Email_body


def Email_LAGDataProcedure(current):   
    #Email parameter
    Email_subject = "LAGData Procedure isn't done on '{time}', please tell Jimmy to check/update it".format(time=current)
    Email_body = "LAGData Procedure isn't done on '{time}', please tell Jimmy to check/update it".format(time=current)
    
    return Email_subject, Email_body


#Condition of Noah1 & Noah2
#by yao saying, there will be status = ExcludeNoah1Noah2 on the day they'll announce.Won't be the status only exclude noah1.
Exclude_Site = 'NotExcludeNoah1Noah2'#['NotExcludeNoah1Noah2', 'ExcludeNoah1', 'ExcludeNoah1Noah2']


#sql table in balance center 190
LookUptable_statustable = "[BalanceOutcome].[dbo].[DS_LookUptableStatus_STATUS]"#DS server：190 and Noah server:254 ,there are same table in these two server.
statustable_columns = ['Updatetime', 'status']

server_lookup_table = "[DataScientist].[dbo].[ds_serverlookup]"
Balance190_lookuptablename = "[BalanceOutcome].[dbo].[LookUpTable]"
Balance190_lookuptablename_new = "[BalanceOutcome].[dbo].[LookUpTableNew]"
lookuptable_col = ['LinkServerName', 'GameTypeSourceId', 'DBName', 'MonthDB', 'TableName', 'Type']
DailyQueryTable_all = "[BalanceCenterSummarize ].[dbo].[DS_BalanceCenterDailyQuery]"

#sql table in JG
JG_TimezoneTable = "[DataScientist].[dbo].[DS_DuizhangTimeZone]"
StatsTable_bytype = "[DataScientist].[dbo].[DS_BalanceCenterDailyQuery_StatusRecordTable]"#"[DataScientist].[dbo].[DS_BalanceCenterDailyQueryStatus_byType]"

StatsTable_bytype_col = ['Server', 'Type', 'Destination', 'Status', 'Exe_Time_sec', 'UpDateTime']#['Server', 'Type', 'Status', 'Exe_Time_sec', 'UpDateTime']
DailyQueryTable = "[DataScientist].[dbo].[DS_BalanceCenterDailyQuery]"

JG_lookuptablename = "[DataScientist].[dbo].[LookUpTable]"

JG_LagDataTable = "[DataScientist].[dbo].[DailyqueryLAGDATASTATUS]"
JG_LagDataTable_status = "[DataScientist].[dbo].[DailyqueryLAGDATASTATUS_STATUS]"

