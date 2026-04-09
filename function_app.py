import logging
import azure.functions as func
import requests

from main import main as script


app = func.FunctionApp()

def notify(msg):
    url = 'https://notificationapp-h0g9dwc7fvfchaaw.westeurope-01.azurewebsites.net/notify'

    error = {
        "error": msg,
        "sourceClient": 'Moovle datacraft daily refresh',
        "type": "Notification",
        #  "time": "test"
    }
    headers = {'key': '8^LQhUF4NR1%EjunPGQ@'}
    requests.post(url, headers=headers, json=error, params={'medium': 'telegram'})
    return



@app.timer_trigger(schedule="0 0 4 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def dailyRefresh(myTimer: func.TimerRequest) -> None:

    try:
        script()
    except Exception as e:
        notify(str(e))  

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
