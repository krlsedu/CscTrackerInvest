from flask import request

from repository.HttpRepository import HttpRepository
from service.Interceptor import Interceptor

http_repository = HttpRepository()


class DividendHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def get_dividends(self, args=None, headers=None):
        filters, values = http_repository.get_filters(args, headers)
        dividends = http_repository.get_objects('dividends', filters, values, headers)
        return dividends

    def add_dividend(self, headers=None, dividend=None):
        if dividend is None:
            dividend = request.get_json()
        stock = http_repository.get_object("stocks", ["ticker"], {"ticker": dividend["ticker"]}, headers)
        try:
            del dividend['ticker']
        except KeyError:
            pass
        dividend['investment_id'] = stock['id']
        http_repository.insert('dividends', dividend, headers)
        return 'OK'
