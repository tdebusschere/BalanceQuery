import pyodbc
import pandas as pd
import logging
import sys
import keyring
logging.basicConfig(filename= 'run.log', level = logging.ERROR) #loggin設定
pd.set_option('display.max_columns', None)


'''
Connect Class/ Function
'''
class DB(object):
    def __init__(self, driver, server, uid, pwd):
        self.driver = driver
        self.server = server
        self.uid = uid
        self.pwd = pwd
        self.conn = None
        self.cur = self._getConnect()
    
    def _getConnect(self):
        try:
            self.conn = pyodbc.connect(driver=self.driver,
                                       server=self.server,
                                       uid=self.uid,
                                       pwd=self.pwd, 
                                       timeout=0,
                                       ApplicationIntent='READONLY')
            #print(self.conn.getinfo())
            cur = self.conn.cursor()
            return cur
        except Exception as ex:
            logging.error('SQL Server connecting error, reason is: {}'.format(str(ex)))
   
    def _getCursor(self):
        try:
           cur = self.conn.cursor()
        except:
           cur = self._getConnect()
        return(cur)
        
    def ExecQuery(self, sql):
        cur = self._getCursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            colList = []
            for colInfo in cur.description:
                colList.append(colInfo[0]) 
            resultList = []
            for row in rows:
                resultList.append(list(row))
            df = pd.DataFrame(resultList, columns=colList)
        except pyodbc.Error as ex:
            logging.error('SQL Server.Error: {}'.format(str(ex)))
            sys.exit()
        cur.close()
        #self.conn.close()
        
        return df

    def Executemany(self, sql, obj):
        cur = self._getCursor()
        try:
            cur.executemany(sql, getattr(obj.values, "tolist", lambda: value)())
            self.conn.commit()
        except pyodbc.Error as ex:
            logging.error('SQL Server.Error: {}'.format(str(ex)))
        cur.close()
    
    def ExecNoQuery(self, sql):
        cur = self._getCursor()
        try:
            cur.execute(sql)
            self.conn.commit()
        except pyodbc.Error as ex:
            logging.error('SQL Server.Error: {}'.format(str(ex)))
        cur.close()
    
        
class JG(DB):
    def __init__(self):
        self.driver = 'SQL Server Native Client 11.0'
        self.server  = 'JG\MSSQLSERVER2016'
        self.uid     = 'DS.Jimmy'
        self.pwd     =  keyring.get_password('JG', self.uid)
        self.cur     =  self._getConnect()


class BalanceCenter_190(DB):
    def __init__(self):
        self.driver = 'SQL Server Native Client 11.0'
        self.server  = '10.80.16.190'
        self.uid     = 'DS.Tom'
        self.pwd     =  keyring.get_password('BalanceCenter_190', self.uid)
        self.cur     =  self._getConnect()


class BalanceCenter_DS(DB):
    def __init__(self):
        self.driver = 'SQL Server Native Client 11.0'
        self.server  = '10.80.16.190'
        self.uid     = 'DS.Tom'
        self.pwd     =  keyring.get_password('BalanceCenter_190', self.uid)
        self.cur     =  self._getConnect()


class BalanceCenter_Noah1(DB):
    def __init__(self):
        self.driver = 'SQL Server Native Client 11.0'
        self.server  = '10.80.26.254'
        self.uid     = 'DS.Tom'
        self.pwd     =  keyring.get_password('BalanceCenter_254', self.uid)
        self.cur     =  self._getConnect()


class BalanceCenter_Noah2(DB):
    def __init__(self):
        self.driver = 'SQL Server Native Client 11.0'
        self.server  = '10.80.26.170'
        self.uid     = 'DS.Tom'
        self.pwd     =  keyring.get_password('BalanceCenter_170', self.uid)
        self.cur     =  self._getConnect()


       
class Duizhang(DB):  
    def __init__(self,ip):
        if ip not in (191, 192, 193, 194):
            return None
        self.driver = 'SQL Server Native Client 11.0'
        self.server = '10.80.16.' + str(ip)
        self.uid    = 'DS.Reader'   
        self.pwd    =  keyring.get_password(self.server, self.uid)
        self.cur    = self._getConnect()
        
