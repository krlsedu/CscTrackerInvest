import logging
from datetime import datetime

import pandas
from csctracker_py_core.repository.http_repository import HttpRepository
from csctracker_py_core.repository.remote_repository import RemoteRepository

from service.stocks_handler import StocksHandler


class FixedIncome:
    def __init__(self,
                 stock_handler: StocksHandler,
                 remote_repository: RemoteRepository,
                 http_repository: HttpRepository):
        self.logger = logging.getLogger()
        self.stock_handler = stock_handler
        self.remote_repository = remote_repository
        self.http_repository = http_repository

    def get_stock_by_ticker(self, ticker_):
        stock_ = {
            'ticker': ticker_
        }
        return self.get_stock(stock_)

    def get_stock(self, movement, headers=None):
        ticker_ = movement['ticker'].upper()
        stock = self.remote_repository.get_object("stocks", ["ticker"], {"ticker": ticker_}, headers)
        try:
            stock['id']
        except Exception as e:
            ticker_ = self.add_stock(movement, headers)
            stock = self.remote_repository.get_object("stocks", ["ticker"], {"ticker": ticker_}, headers)
            self.add_stock_price(stock, headers)
        return stock

    def add_stock(self, movement, headers=None):
        type_ = 16
        try:
            tx_type_ = movement['tx_type']
        except:
            tx_type_ = "CDI"
        code_ = (movement['ticker'].upper().strip() + " - " + str(movement['price']) + " " + tx_type_ + " - " +
                 movement['buy_date'])
        try:
            code_ = code_ + " - " + movement['venc_date']
        except:
            pass
        investment_tp = {
            'ticker': code_,
            'name': code_,
            'investment_type_id': type_,
            'tx_type': tx_type_,
            'tx_quotient': movement['price'],
            'price': 1
        }
        self.remote_repository.insert("stocks", investment_tp, headers)
        return investment_tp['ticker']

    def add_stock_price(self, stock, headers, date=None):
        stock_price = {
            "investment_id": stock['id'],
            "price": float(stock['price']),
            "date_value": "2022-01-01"
        }
        if date is not None:
            stock_price['date_value'] = date
        self.remote_repository.insert("stocks_prices", stock_price, headers)
        try:

            try:
                date_value_ = datetime.strptime(stock_price['date_value'], '%Y-%m-%d').strftime('%Y-%m-%d')
            except:
                date_value_ = datetime.now().strftime('%Y-%m-%d')
            filter = {
                "investment_id": stock_price['investment_id'],
                "date_value": date_value_
            }
            price_agg = self.remote_repository.get_object(
                "stocks_prices_agregated",
                data=filter,
                headers=headers
            )
            if price_agg is not None and price_agg['id'] is not None:
                price_agg['price'] = stock_price['price']
                self.remote_repository.update("stocks_prices_agregated", ["id"], price_agg, headers)
            else:
                stock_price['date_value'] = date_value_
                self.remote_repository.insert("stocks_prices_agregated", stock_price, headers)
        except Exception as e:
            self.logger.info(f"add_price - fixIncome -> {stock_price}")
            self.logger.exception(e)
            pass

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
        price_obj = self.stock_handler.get_price(stock_['id'], date_movement, headers)
        price = float(price_obj['price'])
        type_ = stock_['tx_type']
        if type_ == "IPCA":
            date_mask = "%Y-%m"
        else:
            date_mask = "%Y-%m-%d"
        date_price = datetime.strptime(price_obj['date_value'], '%Y-%m-%d %H:%M:%S.%f').strftime(date_mask)
        if date_price < date_movement:
            date_range = pandas.date_range(date_price, date_movement)
            for date in date_range:
                if date.strftime(date_mask) > date_price:
                    tx_quotient = float(stock_['tx_quotient'] / 100)
                    if type_ == "PRÉ":
                        lt = tx_quotient
                        lq = ((1 + lt) ** (1 / 365))
                    elif type_ == "IPCA":
                        tx_val = self.get_tax_price(type_, date, headers)
                        lt = float(tx_val['value'] / 100)
                        lq = lt * tx_quotient
                        lq = lq + 1
                    else:
                        tx_val = self.get_tax_price(type_, date, headers)
                        lt = float(tx_val['value'] / 100)
                        lt = lt * tx_quotient
                        lq = ((1 + lt) ** (1 / 365))
                    price = price * lq
                    stock_['price'] = float(price)
                    self.add_stock_price(stock_, headers, date.strftime('%Y-%m-%d'))
                    date_price = date.strftime(date_mask)
        else:
            return price_obj
        self.remote_repository.update("stocks", ["id"], stock_, headers)
        return self.stock_handler.get_price(stock_['id'], date_movement, headers)

    def get_tax_price(self, tx_type, date, headers=None):
        select_ = f"select * from taxs where " \
                  f"date_value <= '{date}' " \
                  f"and name = '{tx_type}' " \
                  f"order by date_value desc limit 1"
        objects = self.remote_repository.execute_select(select_, headers)
        return objects[0]
