import json
import locale
import os
from datetime import timedelta, datetime, timezone

import pandas as pd
from flask import request

from repository.HttpRepository import HttpRepository
from repository.Repository import GenericRepository
from service.FiiHandler import FiiHandler
from service.Interceptor import Interceptor
from service.StocksHandler import StocksHandler

generic_repository = GenericRepository()
http_repository = HttpRepository()
fii_handler = FiiHandler()
stock_handler = StocksHandler()


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

        data_ant = datetime.now().astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        price_ant = stock_handler.get_price(stock['id'], data_ant)
        if price_ant is not None:
            if float(price_ant['price']) != float(stock['price']):
                self.add_price(stock_price)
        else:
            self.add_price(stock_price)

    def add_price(self, price):
        try:
            if price['price'] is not None:
                generic_repository.insert("stocks_prices", price)
        except Exception as e:
            print(e)

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

                if investment_type['id'] == 15:
                    data_ant = (datetime.now() - timedelta(2)).strftime('%Y-%m-%d 23:59:59')
                else:
                    data_ant = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d 23:59:59')
                price_ant = stock_handler.get_price(stock_['id'], data_ant)

                segment['type'] = investment_type['name']
                segment['type_id'] = investment_type['id']
                stock_consolidated['type'] = investment_type['name']

                stock_consolidated['price_atu'] = stock_['price']
                if price_ant is not None:
                    stock_consolidated['price_ant'] = price_ant['price']
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
                if price_ant is not None:
                    stock_consolidated['total_value_ant'] = float(stock['quantity']) * float(price_ant['price'])
                    stock_consolidated['value_ant_date'] = price_ant['date_value'].strftime('%Y-%m-%d')
                    stock_consolidated['variation'] = stock_consolidated['total_value_atu'] - \
                                                      stock_consolidated['total_value_ant']
                stock_consolidated['gain'] = float(stock_consolidated['total_value_atu']) / float(
                    stock_consolidated['total_value_invest']) - 1
                stock_consolidated['url_statusinvest'] = "https://statusinvest.com.br" + stock_['url_infos']

                stocks_consolidated.append(stock_consolidated)

                segment['quantity'] = float(stock['quantity'])
                segment['total_value_invest'] = stock_consolidated['total_value_invest']
                segment['total_value_atu'] = stock_consolidated['total_value_atu']

                if price_ant is not None:
                    segment['total_value_ant'] = stock_consolidated['total_value_ant']
                    segment['variation'] = stock_consolidated['variation']

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

    def buy_sell_indication(self):
        perc_ideal = 5
        infos = self.get_stocks_consolidated()

        stocks = infos['stocks']
        type_grouped = infos['type_grouped']
        total_invested = 0
        for type_ in type_grouped:
            total_invested += type_['total_value_atu']

        infos['total_invested'] = total_invested
        stocks_br = stock_handler.get_stocks(1)
        bdrs = stock_handler.get_stocks(4)
        fiis = fii_handler.get_fiis()
        founds = stock_handler.get_founds()
        stock_ref = None
        stock_ = None
        for stock in stocks:
            stock_ = stock
            stock_ref = self.get_stock_ref(bdrs, fiis, stock_, stock_ref, stocks_br)
            self.set_buy_sell_info(perc_ideal, stock_, stock_ref, total_invested, stocks)
        for stock in stocks_br:
            stock_ = stock
            stock_ref = stock
            self.set_buy_sell_info(perc_ideal, stock_, stock_ref, total_invested, stocks)
        for stock in bdrs:
            stock_ = stock
            stock_ref = stock
            self.set_buy_sell_info(perc_ideal, stock_, stock_ref, total_invested, stocks)
        for stock in fiis:
            stock_ = stock
            stock_ref = stock
            self.set_buy_sell_info(perc_ideal, stock_, stock_ref, total_invested, stocks)
        for stock in founds:
            stock_ = stock
            stock_ref = stock
            self.set_buy_sell_info(perc_ideal, stock_, stock_ref, total_invested, stocks)
        infos['stocks'] = stocks
        infos['stocks_br'] = stocks_br
        infos['bdrs'] = bdrs
        infos['fiis'] = fiis
        infos['founds'] = founds
        return infos

    def get_ticket_info(self, ticker, stocks, atr):
        for stock in stocks:
            if stock['ticker'] == ticker:
                return stock[atr]
        return 0

    def set_buy_sell_info(self, perc_ideal, stock_, stock_ref, total_invested, stocks):
        stock_['buy_sell_indicator'] = "neutral"
        if stock_ref is not None:
            try:
                if stock_['perc_atu'] is None:
                    stock_['perc_atu'] = self.get_ticket_info(stock_['ticker'], stocks, 'perc_atu')
            except:
                stock_['perc_atu'] = self.get_ticket_info(stock_['ticker'], stocks, 'perc_atu')
            try:
                if stock_['gain'] is None:
                    stock_['gain'] = self.get_ticket_info(stock_['ticker'], stocks, 'gain')
            except:
                stock_['gain'] = self.get_ticket_info(stock_['ticker'], stocks, 'gain')

            rank = stock_ref['rank']
            if rank <= 20:
                if stock_['perc_atu'] > perc_ideal:
                    sell_rec = self.get_tot_to_sell(total_invested, perc_ideal, stock_['perc_atu'])
                    stock_['recommendation'] = "Sell to equilibrate " + self.to_brl(sell_rec) \
                                               + " -> Rank: " + str(rank)
                else:
                    stock_['buy_sell_indicator'] = "buy"
                    sell_rec = self.get_tot_to_buy(total_invested, perc_ideal, stock_['perc_atu'])
                    stock_['recommendation'] = "Buy to equilibrate " + self.to_brl(sell_rec) \
                                               + " -> Rank: " + str(rank)
            elif rank > 40:
                stock_['buy_sell_indicator'] = "sell"
                stock_['recommendation'] = "Sell all - strategy" \
                                           + " -> Rank: " + str(rank)
            else:
                if stock_['gain'] > 0.2:
                    stock_['buy_sell_indicator'] = "sell"
                    stock_['recommendation'] = "Sell all - great gain -> " + str(stock_['gain'] * 100) + "%"
                if stock_['perc_atu'] > perc_ideal:
                    sell_rec = self.get_tot_to_sell(total_invested, perc_ideal, stock_['perc_atu'])
                    stock_['recommendation'] = "Sell to equilibrate " + self.to_brl(sell_rec) \
                                               + " -> Rank: " + str(rank)
        else:
            if stock_['gain'] > 0.2:
                stock_['buy_sell_indicator'] = "sell"
                stock_['recommendation'] = "Sell all - great gain -> " + str(stock_['gain'] * 100) + "%"
            elif stock_['perc_atu'] > perc_ideal:
                sell_rec = self.get_tot_to_sell(total_invested, perc_ideal, stock_['perc_atu'])
                stock_['recommendation'] = "Sell to equilibrate " + self.to_brl(sell_rec)
            else:
                stock_['recommendation'] = "Sell all - strategy"

    def get_stock_ref(self, bdrs, fiis, stock, stock_ref, stocks_br):
        if stock['investment_type_id'] == 1:
            for obj_ in stocks_br:
                if obj_['ticker'] == stock['ticker']:
                    stock_ref = obj_
                    break
        elif stock['investment_type_id'] == 4:
            for obj_ in bdrs:
                if obj_['ticker'] == stock['ticker']:
                    stock_ref = obj_
                    break
        elif stock['investment_type_id'] == 2:
            for obj_ in fiis:
                if obj_['ticker'] == stock['ticker']:
                    stock_ref = obj_
                    break
        return stock_ref

    def get_tot_to_sell(self, total, perc_ideal, perc_atu):
        valor_atu = total * perc_atu / 100
        novo_total = total - (valor_atu - total * (perc_ideal / 100))
        novo_valor = total * (perc_ideal / 100)
        sell = valor_atu - novo_valor
        val_atu = novo_valor
        while sell > 0.01:
            novo_total = novo_total - (val_atu - novo_total * (perc_ideal / 100))
            novo_valor = novo_total * (perc_ideal / 100)
            sell = val_atu - novo_valor
            val_atu = novo_valor
        return valor_atu - novo_valor

    def get_tot_to_buy(self, total, perc_ideal, perc_atu):
        v_atu = total * perc_atu / 100
        new_tot = total + (total * (perc_ideal / 100) - v_atu)
        new_val = total * (perc_ideal / 100)
        buy = new_val - v_atu
        val_atu = new_val
        while buy > 0.01:
            new_tot = new_tot + (new_tot * (perc_ideal / 100) - val_atu)
            new_val = new_tot * (perc_ideal / 100)
            buy = new_val - val_atu
            val_atu = new_val

        return new_val - v_atu

    def to_brl(self, value):
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
        valor = locale.currency(value, grouping=True, symbol=None)
        return "R$ " + str(valor)

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
