from flask import request
from service.Interceptor import Interceptor

import requests


class RequestHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def inform_to_client(self, json, operation, headers, msg=None):
        message = {
            "text": msg,
            "data": json,
            "app": "CscTrackerInvest",
            "operation": operation
        }
        response = requests.post('http://notify-sync:8890/message', headers=headers, json=message)
        if response.status_code != 200:
            print(response.status_code)
        pass
