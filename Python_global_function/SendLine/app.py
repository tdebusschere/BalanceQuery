import keyring
from flask import Flask, request, abort
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import *

app = Flask(__name__)



# Channel Access Token
line_token = keyring.get_password('Token', 'JimmyYin')
line_sercret = keyring.get_password('sercret', 'JimmyYin')

line_bot_api = LineBotApi(line_token)
handler = WebhookHandler(line_sercret)
user_id = keyring.get_password('user_id', 'JimmyYin')



@app.route("/push_function/<string:push_text_str>")
def push_message(push_text_str):
    line_bot_api.push_message(user_id, TextSendMessage(text=push_text_str))


@app.route("/callback", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']
    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)
    # handle webhook body
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    message = TextSendMessage(text=event.message.text)
    line_bot_api.reply_message(event.reply_token, message)


@app.route('/')
def index():
    return 'Hello World'


import os

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)