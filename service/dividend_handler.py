import logging

from csctracker_py_core.repository.http_repository import HttpRepository
from csctracker_py_core.repository.remote_repository import RemoteRepository


class DividendHandler:
    def __init__(self, remote_repository: RemoteRepository, http_repository: HttpRepository):
        self.logger = logging.getLogger()
        self.remote_repository = remote_repository
        self.http_repository = http_repository

    def get_dividends(self, args=None, headers=None):
        dividends = self.remote_repository.get_objects('dividends', data=args, headers=headers)
        return dividends

    def add_dividend(self, headers=None, dividend=None):
        if dividend is None:
            dividend = self.http_repository.get_json_body()
        stock = self.remote_repository.get_object("stocks", ["ticker"], {"ticker": dividend["ticker"]}, headers)
        try:
            del dividend['ticker']
        except KeyError:
            pass
        dividend['investment_id'] = stock['id']
        self.remote_repository.insert('dividends', dividend, headers)
        return 'OK'
