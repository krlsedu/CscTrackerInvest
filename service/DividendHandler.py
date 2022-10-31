from flask import request

from repository.Repository import GenericRepository
from service.Interceptor import Interceptor

generic_repository = GenericRepository()


class DividendHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def get_dividends(self, args=None, headers=None):
        filters, values = generic_repository.get_filters(args, headers)
        dividends = generic_repository.get_objects('dividends', filters, values)
        return dividends

    def add_dividend(self, headers=None, dividend=None):
        if dividend is None:
            dividend = request.get_json()
        stock = generic_repository.get_object("stocks", ["ticker"], {"ticker": dividend["ticker"]})
        try:
            del dividend['ticker']
        except KeyError:
            pass
        dividend['investment_id'] = stock['id']
        dividend['user_id'] = generic_repository.get_user(headers)['id']
        generic_repository.insert('dividends', dividend)
        return 'OK'
