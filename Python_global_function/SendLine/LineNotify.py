import requests
import keyring

class line(object):
    def __init__(self, Token, headers):
        self.Token = Token#keyring.get_password('Notify_Token', 'JimmyYin')
        self.headers = headers#{"Authorization": "Bearer " + self.Token, 
                        #"Content-Type" : "application/x-www-form-urlencoded"}

    def SendLine(self, msg):
        payload = {'message': msg}
        r = requests.post("https://notify-api.line.me/api/notify",
                          headers=self.headers,
                          params=payload)
       
        return r.status_code

class lineNotifyMessage(line):
    def __init__(self):
        self.Token = keyring.get_password('Notify_Token', 'JimmyYin')
        self.headers = {"Authorization": "Bearer "+ self.Token, 
                        "Content-Type" : "application/x-www-form-urlencoded"}

	
