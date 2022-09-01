import json

import pandas as pd
from flask import request

from model.UserStocks import UserStocks
from repository.HttpRepository import HttpRepository
from repository.Repository import GenericRepository
from service.FiiHandler import FiiHandler
from service.Interceptor import Interceptor

generic_repository = GenericRepository()
http_repository = HttpRepository()
fii_handler = FiiHandler()


class InvestmentHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def add_movements(self, movements):
        msgs = []
        for movement in movements:
            msgs.append(self.add_movement(movement))
        return msgs

    def add_movement(self, movement):
        movement = generic_repository.add_user_id(movement)
        movement_type = generic_repository.get_object("movement_types", ["id"], {"id": movement['movement_type']})
        coef = movement_type['coefficient']
        ticker_ = movement['ticker']
        if ticker_ is not None:
            stock = self.get_stock(ticker_)
            movement['investment_id'] = stock['id']
            movement['investment_type_id'] = stock['investment_type_id']
            del movement['ticker']
        try:
            if generic_repository.exist_by_key("user_stocks", ["investment_id"], movement):
                if movement_type['to_balance']:
                    user_stock = generic_repository.get_object("user_stocks", ["investment_id"], movement)
                    total_value = float(user_stock['quantity'] * user_stock['avg_price'])

                    total_value += movement['quantity'] * movement['price'] * float(coef)
                    quantity = float(user_stock['quantity']) + float(movement['quantity']) * float(coef)
                    if quantity != 0:
                        avg_price = total_value / float(quantity)
                    else:
                        avg_price = 0
                    user_stock['quantity'] = quantity
                    user_stock['avg_price'] = avg_price
                    generic_repository.update("user_stocks", ["user_id", "investment_id"], user_stock)
                generic_repository.insert("user_stocks_movements", movement)
            else:
                if movement_type['to_balance']:
                    stock = {'investment_id': movement['investment_id'], 'quantity': movement['quantity'],
                             'avg_price': movement['price'], 'user_id': movement['user_id'],
                             'investment_type_id': movement['investment_type_id']}
                    generic_repository.insert("user_stocks", stock)
                generic_repository.insert("user_stocks_movements", movement)
            return {"status": "success", "message": "Movement added"}
        except Exception as e:
            print(e)
            return {"status": "error", "message": e}

    def get_stock(self, ticker_):
        ticker_ = ticker_.upper()
        stock = generic_repository.get_object("stocks", ["ticker"], {"ticker": ticker_})
        try:
            stock['id']
        except Exception as e:
            ticker_ = self.add_stock(ticker_)
            stock = generic_repository.get_object("stocks", ["ticker"], {"ticker": ticker_})
            self.add_stock_price(stock)
        return stock

    def add_stock_price(self, stock, date=None):
        stock_price = {
            "investment_id": stock['id'],
            "price": stock['price'],
        }
        if date is not None:
            stock_price['date_value'] = date
        self.add_price(stock_price)

    def add_price(self, price):
        generic_repository.insert("stocks_prices", price)

    def add_stock(self, ticker_):
        stock = http_repository.get_firt_stock_type(ticker_)
        type_ = stock['type']
        code_ = stock['code']
        if type_ == 15:
            code_ = ticker_.upper()
        investment_tp = {
            'ticker': code_,
            'name': stock['name'],
            'investment_type_id': type_,
            'url_infos': stock['url']
        }
        generic_repository.insert("stocks", investment_tp)
        return investment_tp['ticker']

    def get_stocks(self):
        user_id = generic_repository.get_user()['id']
        args = request.args
        filters = ["user_id"]
        values = {"user_id": user_id}
        for key in args:
            filters.append(key)
            values[key] = args[key]
        return generic_repository.get_objects("user_stocks", filters, values)

    def get_stocks_consolidated(self):
        stocks = self.get_stocks()
        if stocks.__len__() > 0:
            stocks_consolidated = []
            segments = []
            for stock in stocks:
                segment = {}
                stock_consolidated = {}
                investment_type = generic_repository.get_object("stocks", ["id"],
                                                                {"id": stock['investment_id']})
                ticker_ = investment_type['ticker']
                stock['ticker'] = ticker_
                stock_ = generic_repository.get_object("stocks", ["ticker"], stock)
                stock_, investment_type = http_repository.get_values_by_ticker(stock_)

                segment['type'] = investment_type['name']
                segment['type_id'] = investment_type['id']
                stock_consolidated['type'] = investment_type['name']

                stock_consolidated['price_atu'] = stock_['price']
                stock_consolidated['segment'] = stock_['segment']
                segment['segment'] = stock_['segment']

                stock_consolidated['ticker'] = ticker_
                stock_consolidated['investment_type_id'] = investment_type['id']
                stock_consolidated['name'] = stock_['name']
                stock_consolidated['quantity'] = stock['quantity']
                stock_consolidated['avg_price'] = stock['avg_price']
                stock_consolidated['total_value_invest'] = stock['quantity'] * stock['avg_price']
                stock_consolidated['total_value_atu'] = float(stock['quantity']) * \
                                                        float(stock_consolidated['price_atu'])
                stock_consolidated['gain'] = float(stock_consolidated['total_value_atu']) / float(
                    stock_consolidated['total_value_invest']) - 1
                stock_consolidated['url_statusinvest'] = "https://statusinvest.com.br" + stock_['url_infos']
                stocks_consolidated.append(stock_consolidated)

                segment['quantity'] = float(stock['quantity'])
                segment['total_value_invest'] = float(stock['quantity'] * stock['avg_price'])
                segment['total_value_atu'] = float(stock['quantity']) * float(stock_consolidated['price_atu'])
                segments.append(segment)

            df_stocks = self.sum_data(stocks_consolidated)
            df_grouped_segment = self.sum_data(segments)
            df_grouped_type = self.sum_data(segments)

            ret = {
                'stocks': json.loads(df_stocks.to_json(orient="records"))
            }

            df_grouped_segment = self.group_data(df_grouped_segment, 'segment')
            df_grouped_type = self.group_data(df_grouped_type, ['type', 'type_id'])
            ret['segments_grouped'] = json.loads(df_grouped_segment.to_json(orient="table"))['data']
            ret['type_grouped'] = json.loads(df_grouped_type.to_json(orient="table"))['data']
            # self.att_stocks_ranks()
            return ret
        else:
            return []

    def sum_data(self, data):
        df = pd.DataFrame.from_dict(data)
        value_invest_sum = df['total_value_invest'].sum()
        value_atu_sum = df['total_value_atu'].sum()
        df['perc_invest'] = (df['total_value_invest'] / value_invest_sum) * 100
        df['perc_atu'] = (df['total_value_atu'] / value_atu_sum) * 100
        return df

    def group_data(self, df, group_by):
        return df.groupby(group_by).sum()

    def calc_ranks(self, stocks):
        df = pd.DataFrame.from_dict(stocks)
        df['rank_dy'] = df['dy'].rank(ascending=False)
        df['rank_pvp'] = df['pvp'].rank(ascending=True)
        df['rank_desv_dy'] = df['desv_dy'].rank(ascending=True)
        df['rank_pl'] = df['pl'].rank(ascending=True)

        return json.loads(df.to_json(orient="records"))

    def att_stocks_ranks(self):
        stocks = generic_repository.get_objects("stocks", [], {})
        stocks = self.calc_ranks(stocks)
        for stock in stocks:
            generic_repository.update("stocks", ["id"], stock)
