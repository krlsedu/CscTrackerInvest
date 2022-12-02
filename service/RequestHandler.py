import datetime
import decimal
import json

import requests

from service.Interceptor import Interceptor


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f')
        if isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')


class RequestHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def inform_to_client(self, json_, operation, headers, msg=None, from_=None):

        try:
            json_ = json.dumps(json_, cls=Encoder, ensure_ascii=False)
        except Exception as e:
            print(e)
            pass

        message = {
            "text": msg,
            "data": json_,
            "app": "CscTrackerInvest",
            "operation": operation
        }

        if from_ is not None:
            message['from'] = from_

        response = requests.post('http://bff:8080/notify-sync/message', headers=headers, json=message)
        if response.status_code != 200:
            print(response.status_code)
        pass
