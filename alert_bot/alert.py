from dingtalkchatbot.chatbot import DingtalkChatbot

def send_cleaner_error(bot_auth, message):
    """DingTalk Chat incoming webhook quickstart."""
    webhook = 'https://oapi.dingtalk.com/robot/send?access_token={}'.format(bot_auth['access_token'])
    chatbot = DingtalkChatbot(webhook, secret=bot_auth['secret']) 
    chatbot.send_text(msg=message, at_mobiles=[bot_auth['tag']])