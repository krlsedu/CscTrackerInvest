
from datetime import datetime

import pandas

from repository.HttpRepository import HttpRepository
from service.Interceptor import Interceptor
from service.StocksHandler import StocksHandler

http_repository = HttpRepository()
stock_handler = StocksHandler()


class FixedIncome(Interceptor):
    def __init__(self):
        super().__init__()

    def get_stock_by_ticker(self, ticker_):
        stock_ = {
            'ticker': ticker_
        }
        return self.get_stock(stock_)

    def get_stock(self, movement, headers=None):
        ticker_ = movement['ticker'].upper()
        stock = http_repository.get_object("stocks", ["ticker"], {"ticker": ticker_}, headers)
        try:
            stock['id']
        except Exception as e:
            ticker_ = self.add_stock(movement, headers)
            stock = http_repository.get_object("stocks", ["ticker"], {"ticker": ticker_}, headers)
            self.add_stock_price(stock, headers)
        return stock

    def add_stock(self, movement, headers=None):
        type_ = 16
        code_ = movement['ticker'].upper().strip()
        try:
            tx_type_ = movement['tx_type']
        except:
            tx_type_ = "CDI"
        investment_tp = {
            'ticker': code_,
            'name': movement['ticker'],
            'investment_type_id': type_,
            'tx_type': tx_type_,
            'tx_quotient': movement['price'],
            'price': 1
        }
        http_repository.insert("stocks", investment_tp, headers)
        return investment_tp['ticker']

    def add_stock_price(self, stock, headers, date=None):
        stock_price = {
            "investment_id": stock['id'],
            "price": float(stock['price']),
            "date_value": "2022-01-01"
        }
        if date is not None:
            stock_price['date_value'] = date
        http_repository.insert("stocks_prices", stock_price, headers)

    def get_stock_price_by_ticker(self, ticker_, headers, date=None):
        stock_ = {
            'ticker': ticker_
        }
        if date is not None:
            stock_['buy_date'] = date
        return self.get_stock_price(stock_, headers)

    def get_stock_price(self, movement, headers=None):
        stock_ = self.get_stock(movement, headers)
        date_movement = movement['buy_date']
        price_obj = stock_handler.get_price(stock_['id'], date_movement, headers)
        price = float(price_obj['price'])
        date_price = datetime.strptime(price_obj['date_value'], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d')
        if date_price < date_movement:
            date_range = pandas.date_range(date_price, date_movement)
            for date in date_range:
                if date.strftime('%Y-%m-%d') > date_price:
                    tx_val = self.get_tax_price(stock_['tx_type'], date, headers)
                    lt = float(tx_val['value'] / 100)
                    lq = ((1 + lt) ** (1 / 365))
                    price = price * lq
                    stock_['price'] = float(price)
                    self.add_stock_price(stock_, headers, date.strftime('%Y-%m-%d'))
        else:
            return price_obj
        http_repository.update("stocks", ["id"], stock_, headers)
        return stock_handler.get_price(stock_['id'], date_movement)

    def get_tax_price(self, tx_type, date, headers=None):
        select_ = f"select * from taxs where " \
                  f"date_value <= '{date}' " \
                  f"and name = '{tx_type}' " \
                  f"order by date_value desc limit 1"
        objects = http_repository.execute_select(select_, headers)
        return objects[0]
