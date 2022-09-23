from datetime import datetime

import pandas

from repository.Repository import GenericRepository
from service.Interceptor import Interceptor
from service.StocksHandler import StocksHandler

generic_repository = GenericRepository()
stock_handler = StocksHandler()


class FixedIncome(Interceptor):
    def __init__(self):
        super().__init__()

    def get_stock_by_ticker(self, ticker_):
        stock_ = {
            'ticker': ticker_
        }
        return self.get_stock(stock_)

    def get_stock(self, movement):
        ticker_ = movement['ticker'].upper()
        stock = generic_repository.get_object("stocks", ["ticker"], {"ticker": ticker_})
        try:
            stock['id']
        except Exception as e:
            ticker_ = self.add_stock(movement)
            stock = generic_repository.get_object("stocks", ["ticker"], {"ticker": ticker_})
            self.add_stock_price(stock)
        return stock

    def add_stock(self, movement):
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
        generic_repository.insert("stocks", investment_tp)
        return investment_tp['ticker']

    def add_stock_price(self, stock, date=None):
        stock_price = {
            "investment_id": stock['id'],
            "price": float(stock['price']),
            "date_value": "2022-01-01"
        }
        if date is not None:
            stock_price['date_value'] = date
        generic_repository.insert("stocks_prices", stock_price)

    def get_stock_price_by_ticker(self, ticker_, date=None):
        stock_ = {
            'ticker': ticker_
        }
        if date is not None:
            stock_['buy_date'] = date
        return self.get_stock_price(stock_)

    def get_stock_price(self, movement):
        stock_ = self.get_stock(movement)
        date_movement = movement['buy_date']
        price_obj = stock_handler.get_price(stock_['id'], date_movement)
        price = float(price_obj['price'])
        date_price = price_obj['date_value'].strftime('%Y-%m-%d')
        if date_price < date_movement:
            date_range = pandas.date_range(date_price, date_movement)
            for date in date_range:
                tx_val = self.get_tax_price(stock_['tx_type'], date)
                lt = float(tx_val['value'] / 100)
                lq = ((1 + lt) ** (1 / 365))
                price = price * lq
                stock_['price'] = float(price)
                self.add_stock_price(stock_, date.strftime('%Y-%m-%d'))
        else:
            return price_obj
        generic_repository.update("stocks", ["id"], stock_)
        return stock_handler.get_price(stock_['id'], date_movement)

    def get_tax_price(self, tx_type, date):
        select_ = f"select * from taxs where " \
                  f"date_value <= '{date}' " \
                  f"and name = '{tx_type}' " \
                  f"order by date_value desc limit 1"
        cursor, cursor_ = generic_repository.execute_select(select_)
        col_names = cursor.description
        obj = {}
        for row in cursor_:
            i = 0
            for col_name in col_names:
                obj[col_name[0]] = row[i]
                i += 1
            cursor.close()
            return obj

        cursor.close()
