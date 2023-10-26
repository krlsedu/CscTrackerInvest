import datetime
import decimal
import json
import os
import threading
from datetime import datetime

import requests as requests


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f')
        if isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')


from service.Interceptor import Interceptor


class Utils(Interceptor):
    def __init__(self):
        super().__init__()

    def work_time(self):
        n = datetime.now()
        t = n.timetuple()
        y, m, d, h, min, sec, wd, yd, i = t
        h = h - 3
        return 8 <= h <= 19

    def work_day(self):
        n = datetime.now()
        t = n.timetuple()
        y, m, d, h, min, sec, wd, yd, i = t
        return wd < 5

    def read_file(self, file_name):
        file_dir = os.path.dirname(os.path.realpath('__file__'))
        file_name = os.path.join(file_dir, file_name)
        file_handle = open(file_name)
        content = file_handle.read()
        file_handle.close()
        return content

    @staticmethod
    def inform_to_client_tr(json_, operation, headers, msg=None, from_=None):

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

        try:
            response = requests.post('http://bff:8080/notify-sync/message', headers=headers, json=message)
            if response.status_code < 200 or response.status_code > 299:
                print(response.status_code)
            pass
        except Exception as e:
            print(e)
            pass

    @staticmethod
    def inform_to_client(json_, operation, headers, msg=None, from_=None):
        threading.Thread(target=Utils.inform_to_client_tr, args=(json_, operation, headers, msg, from_)).start()
