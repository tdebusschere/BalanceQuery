import keyring
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart


def get_email_to_string(JG, condition='BalanceCenter_DS'):
    
    if condition == 'BalanceCenter_DSplusDBA':
        sqlquery_email_string = "SELECT Email FROM DataScientist.dbo.DS_EmailControl \
                        where [BalanceCenter] = 'Send'"
    elif condition == 'BalanceCenter_DS':
        sqlquery_email_string = "SELECT Email FROM DataScientist.dbo.DS_EmailControl \
                        where [Department] = 'DS' AND [BalanceCenter] = 'Send'"
    elif condition == 'GameLookup':
        sqlquery_email_string = "SELECT Email FROM DataScientist.dbo.DS_EmailControl \
                                where GameLookup = 'Send'"            
    elif condition == 'Self':
        sqlquery_email_string = "SELECT Email FROM DataScientist.dbo.DS_EmailControl \
                        where [Department] = 'DS' AND [name] = 'Jimmy'"                        
    To_string_df = JG.ExecQuery(sqlquery_email_string)

    

    To_string = str(list(To_string_df.Email))
    To_string = To_string.replace("'", "")
    To_string = To_string.replace("[", "")
    To_string = To_string.replace("]", "")
    
    return To_string



class BaseEmail(object):
    def __init__(self, gmail_user, gmail_password):
        self.gmail_user = gmail_user
        self.gmail_password = gmail_password#keyring.get_password('Gmail', self.gmail_user)

    def __getConnect(self):
        self.conn = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server = self.conn
        server.ehlo()
        server.login(self.gmail_user, self.gmail_password)
        return server

    def SendMessage(self, subject, text, To_string=None, file=None):
        server = self.__getConnect()
        msg = MIMEMultipart()
        body = MIMEText(text)
        msg.attach(body)
        if file is None:
            pass
        else:
            part_attach1 = MIMEApplication(open(file, 'rb').read())  # 開啟附件
            part_attach1.add_header('Content-Disposition', 'attachment', filename=file)  # 為附件命名
            msg.attach(part_attach1)  # 新增附件
        msg['Subject'] = subject
        msg['From'] = self.gmail_user
        
        msg['To'] = ('jimmy19910620@hotmail.com' if To_string == None else To_string)
        server.send_message(msg)
        server.quit()

class Email(BaseEmail):
    def __init__(self):
        self.gmail_user = keyring.get_password('Gmail_Send', 'Gmail_Sendaccount')#keyring.get_password('Notify_Token', 'JimmyYin')
        self.gmail_password = keyring.get_password('Gmail', self.gmail_user)





