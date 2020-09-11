import keyring
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart


class Email:
    def __init__(self, gmail_user):
        self.gmail_user = gmail_user
        self.gmail_password = keyring.get_password('Gmail', self.gmail_user)

    def __getConnect(self):
        self.conn = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server = self.conn
        server.ehlo()
        server.login(self.gmail_user, self.gmail_password)
        return server

    def SendMessage(self, subject, text, file=None):
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
        msg['To'] = 'jimmy19910620@hotmail.com'+ ',' + 'tom_tong@xinwang.com.tw' + ',' + 'yan_lai@xinliwang.com.tw'
        server.send_message(msg)
        server.quit()
        
