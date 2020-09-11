import numpy as np
import pandas as pd
import sys

####Hourly
#record for this lagdata hour status 
def record_lagdata_Hourlystatus(current, JG, status):
    
    if status == 'Insert_Updating':
        
        record_updating = pd.DataFrame([[current, 'Updating']], columns=['DatePlayed', 'Status'])
        JG.Executemany("insert into DataScientist.dbo.DailyqueryLAGDATASTATUS_STATUS(\
                        [DatePlayed], [Status]) values (?,?)", record_updating)   
        
    elif status == 'Update_to_Success':
        
        JG.ExecNoQuery("update DataScientist.dbo.DailyqueryLAGDATASTATUS_STATUS \
                       set [Status] = 'Success' \
                       where [DatePlayed] = '{current}'".format(current=current))  
    
#get hourly alltype structure    
def get_hourly_alltype_structure(**arg):
    #variable
    IP190, current, target_type = arg['Connect'], arg['now_tw_time'], arg['TA_type']
    
    
    SQLString = " SELECT DISTINCT lt.Type\
        				,lt.LinkServerName\
                        ,gl.category \
                  FROM   (SELECT type, LinkServerName \
                          FROM   [BalanceOutcome].[dbo].[LookUpTableNew]) lt \
                 LEFT JOIN [DataPool].[dbo].[vw_gamelookup] gl \
                 ON lt.type = gl.rawdatatype ORDER  BY lt.type "
    df = IP190.ExecQuery(SQLString)
    df.loc[:, 'DatePlayed'] = current
    df.loc[:, 'Status'] = np.where(df.Type.isin(target_type), 'TBD', 'Success')      
    
    return df

##Insert data
#insert_not_inTAType
def insert_not_inTAType(**arg):
    #variable        
    df, target_type, JG  = arg['structute'], arg['TA_type'], arg['Connect']
    
    #process
    df_final_normal = df[~df.Type.isin(target_type)][['DatePlayed', 'LinkServerName', 'Type', \
                                                    'category', 'Status']].reset_index(drop=True)
    JG.Executemany("insert into DataScientist.dbo.DailyqueryLAGDATASTATUS(\
                    [DatePlayed], [LinkServerName], [Type] , [Category], \
                    [Status]) values (?,?,?,?,?)", df_final_normal) 
    


#insert_inTAType
def insert_inTAType(**arg):
    #variable        
    df, target_type, JG, utc  = arg['structute'], arg['TA_type'], arg['Connect'], arg['utc']
    
    #process    
    df_final_ta = df[df.Type.isin(target_type)].copy()    

    SQLString_TA = "SELECT A.Type,\
                           A.table_max,\
                           DATEADD(minute, B.timezone*60-10, '{utc}') [Run_threshold], \
                           B.timezone\
                   FROM (SELECT [type], \
                                 [table_max],\
                                 ROW_NUMBER() OVER(PARTITION BY [type] ORDER BY [batch_time] desc) rowNumber\
                         FROM   [DataScientist].[dbo].[ds_timezonevalue]\
                         WHERE  type IN {target_type})A \
                   LEFT JOIN [DataScientist].[dbo].[ds_duizhangtimezone] B \
                   ON A.type = B.type where a.rowNumber = 1 ORDER  BY a.type ".format(target_type=str(tuple(target_type)),
                                                                                      utc=utc)        
    threshold = JG.ExecQuery( SQLString_TA )
        
    df_final_ta = df_final_ta.merge(threshold[['Type', 'table_max', 'Run_threshold']],
                                    how='left',
                                    on = 'Type')
    df_final_ta.loc[df_final_ta[df_final_ta.table_max >= df_final_ta.Run_threshold].index, 'Status'] = 'Success'
    df_final_ta.loc[df_final_ta[df_final_ta.table_max < df_final_ta.Run_threshold].index, 'Status'] = 'Wait'
    df_final_ta = df_final_ta[['DatePlayed', 'LinkServerName', 'Type', \
                                'category', 'Status', 'table_max',\
                                'Run_threshold']].reset_index(drop=True)  
    
    JG.Executemany("insert into DataScientist.dbo.DailyqueryLAGDATASTATUS(\
                    [DatePlayed], [LinkServerName], [Type] , [Category],\
                    [Status], [Table_max], [Run_threshold]) values (?,?,?,?,?,?,?)", df_final_ta)    
 
    
  

    
##condition happended then mail to aware that.
def condition_happen_email(**arg):
    #variable        
    execute, To_string, target_type, JG, Threshold_times  = arg['email_class'], arg['To_string'], arg['TA_type'], arg['Connect'], arg['Threshold_times']
    
    #process    
    for j in target_type:
        
        sqlstring = "select * from [DataScientist].[dbo].[DailyqueryLAGDATASTATUS]\
                    where type ={type} and status = 'wait'\
                    order by dateplayed desc".format(type=j)
        temp = JG.ExecQuery( sqlstring )
        if  temp.shape[0] >= Threshold_times:   
            
            zmm = JG.ExecQuery("select min(dateplayed) from [DataScientist].[dbo].[DailyqueryLAGDATASTATUS]\
                               where type ={type} and status = 'wait'".format(type=j)).iloc[0]
            zmm = zmm.iloc[0].strftime("%Y-%m-%d %H:00:00.000")
            Email_subject, Email_body = "The issue of insert lag in the Balance center.",\
                                        "Type={type} is not updating since '{time}', please be careful about this".format(type=j, time=zmm)
            execute.SendMessage(Email_subject,
                                Email_body, 
                                To_string)
            
        else:
            continue     


####Ten minutes
def update_Tablemax_and_Status(rerun_df, JG):
    if rerun_df.empty:
        sys.exit()
    else:
        target_type = np.unique(rerun_df.Type)
        target_type_string = ('('+str(rerun_df.Type.iloc[0]) + ')' if len(target_type)==1 else str(tuple(target_type)))
        
        SQLString_TA = "SELECT A.Type, A.table_max\
                        FROM (SELECT [type], [table_max],\
                                      ROW_NUMBER() OVER(PARTITION BY [type] ORDER BY [batch_time] desc) rowNumber\
                              FROM   [DataScientist].[dbo].[ds_timezonevalue]\
                              WHERE  type IN {target_type})A \
                        where a.rowNumber = 1 ORDER  BY a.type ".format(target_type=target_type_string)
        
        condition_now = JG.ExecQuery(SQLString_TA)
        
        
        #UPDATE TABLE MAX
        for i in range(rerun_df.shape[0]):
            type_iter, threshold_iter = rerun_df.Type[i], str(rerun_df.Run_threshold[i])
            update_table_max = str(condition_now[condition_now.Type == type_iter].table_max.iloc[0])[0:23]
            JG.ExecNoQuery("UPDATE DataScientist.dbo.DailyqueryLAGDATASTATUS \
                           set Table_max='{update_table_max}' \
                           where type = {type_iter} and Run_threshold = '{threshold_iter}'".format(type_iter=type_iter,
                                                                                                 update_table_max=update_table_max,
                                                                                                 threshold_iter=threshold_iter))
            
        JG.ExecNoQuery("UPDATE DataScientist.dbo.DailyqueryLAGDATASTATUS set STATUS = 'ReRun'\
                       WHERE STATUS = 'Wait' AND TABLE_MAX >= Run_threshold")      
