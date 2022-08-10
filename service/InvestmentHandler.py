import json

import pandas as pd

from model.UserStocks import UserStocks
from repository.Repository import GenericRepository
from service.FiiHandler import FiiHandler
from service.Interceptor import Interceptor

generic_repository = GenericRepository()
fii_handler = FiiHandler()


class InvestmentHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def add_movement(self, movement):
        movement = generic_repository.add_user_id(movement)
        movement_type = generic_repository.get_object("movement_types", ["id"], {"id": movement['movement_type']})
        ticker_ = movement['ticker']
        if ticker_ is not None:
            investment_type = generic_repository.get_object("investment_types", ["ticker"], {"ticker": ticker_})
            try:
                id_ = investment_type['id']
            except Exception as e:
                id_ = None
            if id_ is None:
                investment_tp = {'ticker': ticker_, 'name': ticker_}
                generic_repository.insert("investment_types", investment_tp)
            investment_type = generic_repository.get_object("investment_types", ["ticker"], {"ticker": ticker_})
            movement['investment_id'] = investment_type['id']
            del movement['ticker']
        coef = movement_type['coefficient']
        try:
            stock = {'investment_id': movement['investment_id'], 'quantity': movement['quantity'],
                     'avg_price': movement['price'], 'user_id': movement['user_id']}
            if generic_repository.exist_by_key("user_stocks", ["investment_id"], movement):
                user_stock = generic_repository.get_object("user_stocks", ["investment_id"], movement)
                total_value = float(user_stock['quantity'] * user_stock['avg_price'])

                total_value += movement['quantity'] * movement['price'] * float(coef)
                quantity = user_stock['quantity'] + movement['quantity'] * coef
                if quantity != 0:
                    avg_price = total_value / float(quantity)
                else:
                    avg_price = 0
                user_stock['quantity'] = quantity
                user_stock['avg_price'] = avg_price
                generic_repository.update("user_stocks", ["user_id", "investment_id"], user_stock)
                generic_repository.insert("user_stocks_movements", movement)
            else:
                generic_repository.insert("user_stocks", stock)
                generic_repository.insert("user_stocks_movements", movement)
        except Exception as e:
            print(e)

    def get_stocks(self):
        user_id = generic_repository.get_user()['id']
        return generic_repository.get_objects("user_stocks", ["user_id"], {"user_id": user_id})

    def get_stocks_consolidated(self):
        stocks = self.get_stocks()
        if stocks.__len__() > 0:
            stocks_consolidated = []
            for stock in stocks:
                stock_consolidated = {}
                investment_type = generic_repository.get_object("investment_types", ["id"],
                                                                {"id": stock['investment_id']})
                ticker_ = investment_type['ticker']
                fii = fii_handler.get_fii({'ticker': ticker_})
                try:
                    stock_consolidated['price_atu'] = float(fii['price'])
                except Exception as e:
                    pass
                stock_consolidated['ticker'] = ticker_
                stock_consolidated['quantity'] = stock['quantity']
                stock_consolidated['avg_price'] = stock['avg_price']
                stock_consolidated['total_value_invest'] = stock['quantity'] * stock['avg_price']
                stock_consolidated['total_value_atu'] = float(stock['quantity']) * stock_consolidated['price_atu']
                stocks_consolidated.append(stock_consolidated)

            df = pd.DataFrame.from_dict(stocks_consolidated)
            value_invest_sum = df['total_value_invest'].sum()
            value_atu_sum = df['total_value_atu'].sum()
            df['perc_invest'] = (df['total_value_invest'] / value_invest_sum) * 100
            df['perc_atu'] = (df['total_value_atu'] / value_atu_sum) * 100
            return json.loads(df.to_json(orient="records"))
        else:
            return []
