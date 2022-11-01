import json
from datetime import timedelta, datetime, timezone

import pandas as pd
from flask import request

from repository.HttpRepository import HttpRepository
from repository.Repository import GenericRepository
from service.DividendHandler import DividendHandler
from service.FiiHandler import FiiHandler
from service.FixedIncome import FixedIncome
from service.Interceptor import Interceptor
from service.StocksHandler import StocksHandler

generic_repository = GenericRepository()
http_repository = HttpRepository()
fii_handler = FiiHandler()
stock_handler = StocksHandler()
fixed_income_handler = FixedIncome()
dividend_handler = DividendHandler()


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
            try:
                fixed_icome = movement['fixed_icome']
            except:
                fixed_icome = "N"
            if fixed_icome != "S":
                stock = self.get_stock(ticker_)
            else:
                stock = fixed_income_handler.get_stock(movement)
                price = fixed_income_handler.get_stock_price(movement)
                movement['price'] = float(price['price'])
                movement['quantity'] = float(movement['quantity']) / movement['price']
            try:
                del movement['fixed_icome']
            except:
                pass
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
                    profit_loss_value = float(movement['price']) - float(user_stock['avg_price'])
                    user_stock['avg_price'] = avg_price
                    if movement['movement_type'] == 2:
                        self.add_profit_loss(profit_loss_value, movement)
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

    def add_profit_loss(self, profit_loss_value, movement):
        try:
            date = movement['date']
        except:
            date = datetime.now().strftime('%Y-%m-%d')
        profit_loss = {
            "user_id": movement['user_id'],
            "investment_id": movement['investment_id'],
            "value": profit_loss_value,
            "quantity": movement['quantity'],
            "date_sell": date
        }
        generic_repository.insert("profit_loss", profit_loss)

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

    def get_stocks(self, args=None, headers=None):
        user_id = generic_repository.get_user(headers)['id']
        if args is None:
            args = request.args
        filters = ["user_id"]
        values = {"user_id": user_id}
        for key in args:
            filters.append(key)
            values[key] = args[key]
        return generic_repository.get_objects("user_stocks", filters, values)

    def get_stocks_consolidated(self, args=None, headers=None):
        stocks = self.get_stocks(args, headers)
        if stocks.__len__() > 0:
            stocks_consolidated = []
            segments = []
            for stock in stocks:
                if stock['quantity'] > 0:
                    segment = {}
                    stock_consolidated = {}
                    investment_type = generic_repository.get_object("stocks", ["id"],
                                                                    {"id": stock['investment_id']})
                    ticker_ = investment_type['ticker']
                    stock['ticker'] = ticker_
                    stock_ = generic_repository.get_object("stocks", ["ticker"], stock)
                    stock_, investment_type = http_repository.get_values_by_ticker(stock_)

                    if investment_type['id'] == 16:
                        stock_price = fixed_income_handler \
                            .get_stock_price_by_ticker(ticker_, (datetime.now()).strftime('%Y-%m-%d'))
                        stock_['price'] = stock_price['price']
                    data_ant = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d 23:59:59')
                    price_ant = stock_handler.get_price(stock_['id'], data_ant)
                    data_atu = datetime.now().strftime('%Y-%m-%d 23:59:59')
                    price_atu = stock_handler.get_price(stock_['id'], data_atu)

                    segment['type'] = investment_type['name']
                    segment['type_id'] = investment_type['id']
                    stock_consolidated['type'] = investment_type['name']

                    stock_consolidated['price_atu'] = stock_['price']
                    if price_ant is not None:
                        stock_consolidated['price_ant'] = price_ant['price']
                        stock_consolidated['price_atu'] = price_atu['price']
                        dt_prc_ant = price_ant['date_value'].strftime('%Y-%m-%d')
                        dt_prc_req = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
                        if dt_prc_ant < \
                                dt_prc_req:
                            if investment_type['id'] == 15:
                                self.att_stock_price_new(False, stock_, stock_, "fundo", "1", True, dt_prc_ant)
                            elif investment_type['id'] == 1:
                                self.att_stock_price_new(False, stock_, stock_, "acao", "1", True, dt_prc_ant)
                            elif investment_type['id'] == 2:
                                self.att_stock_price_new(False, stock_, stock_, "fii", "1", True, dt_prc_ant)
                            elif investment_type['id'] == 4:
                                self.att_stock_price_new(False, stock_, stock_, "bdr", "1", True, dt_prc_ant)
                            pass

                    stock_consolidated['segment'] = stock_['segment']
                    segment['segment'] = stock_['segment']

                    stock_consolidated['ticker'] = ticker_
                    stock_consolidated['investment_type_id'] = investment_type['id']
                    stock_consolidated['name'] = stock_['name']
                    stock_consolidated['quantity'] = stock['quantity']
                    stock_consolidated['avg_price'] = stock['avg_price']
                    stock_consolidated['total_value_invest'] = stock['quantity'] * stock['avg_price']
                    if stock_consolidated['investment_type_id'] == 16 or stock_consolidated['investment_type_id'] == 15:
                        perc_gain = float(stock_consolidated['price_atu']) - float(stock_consolidated['avg_price'])

                        stock_consolidated['total_value_atu'] = float(stock_consolidated['total_value_invest']) + \
                                                                float(
                                                                    perc_gain * float(
                                                                        stock_consolidated['total_value_invest']))

                        if price_ant is not None:
                            perc_gain_ant = float(price_ant['price']) - float(stock_consolidated['avg_price'])

                            stock_consolidated['total_value_ant'] = float(stock_consolidated['total_value_invest']) + \
                                                                    float(
                                                                        perc_gain_ant * float(stock_consolidated[
                                                                                                  'total_value_invest']))

                            stock_consolidated['value_ant_date'] = price_ant['date_value'].strftime('%Y-%m-%d')
                            stock_consolidated['variation'] = stock_consolidated['total_value_atu'] - \
                                                              stock_consolidated['total_value_ant']
                    else:
                        stock_consolidated['total_value_atu'] = float(stock['quantity']) * \
                                                                float(stock_consolidated['price_atu'])
                        if price_ant is not None:
                            stock_consolidated['total_value_ant'] = float(stock['quantity']) * float(price_ant['price'])
                            stock_consolidated['value_ant_date'] = price_ant['date_value'].strftime('%Y-%m-%d')
                            stock_consolidated['variation'] = stock_consolidated['total_value_atu'] - \
                                                              stock_consolidated['total_value_ant']
                    stock_consolidated['gain'] = float(stock_consolidated['total_value_atu']) / float(
                        stock_consolidated['total_value_invest']) - 1
                    infos_ = stock_['url_infos']
                    if infos_ is not None:
                        stock_consolidated['url_statusinvest'] = "https://statusinvest.com.br" + infos_

                    stock_consolidated = self.add_dividend_info(stock_consolidated, headers)
                    stock_consolidated = self.add_daily_gain(stock_consolidated, headers)

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

    def get_sotcks_infos(self, args=None, headers=None):
        stocks_br = stock_handler.get_stocks(1, args)
        bdrs = stock_handler.get_stocks(4, args)
        fiis = fii_handler.get_fiis(args)
        founds = stock_handler.get_founds(15)
        fix_income = stock_handler.get_founds(16)
        return stocks_br, bdrs, fiis, founds, fix_income

    def buy_sell_indication(self, args=None, headers=None):
        infos = self.get_stocks_consolidated(args, headers)
        infos['stocks_names'] = stock_handler.get_stocks_basic()

        stocks = infos['stocks']
        type_grouped = infos['type_grouped']
        total_invested = 0
        types_sum = {}
        types_count = {}
        for type_ in type_grouped:
            total_invested += type_['total_value_atu']
            types_sum[type_['type_id']] = type_['total_value_atu']
            types_count[type_['type_id']] = 0
        segment_grouped = infos['segments_grouped']
        segments_sum = {}
        for segment in segment_grouped:
            segments_sum[segment['segment']] = segment['total_value_atu']
        types_sum[0] = total_invested
        types_sum['segment_sum'] = segments_sum
        types_sum['types_count'] = types_count
        infos['total_invested'] = total_invested

        stocks_br, bdrs, fiis, founds, fix_income = self.get_sotcks_infos(args, headers)
        stock_ref = None
        for stock in stocks:
            stock_ = stock
            types_sum['types_count'][stock['investment_type_id']] += 1
            stock_ref = self.get_stock_ref(bdrs, fiis, stock_, stock_ref, stocks_br, founds, fix_income)
            self.set_buy_sell_info(stock_, stock_ref, types_sum, stocks)
        for stock in stocks_br:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks)
        for stock in bdrs:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks)
        for stock in fiis:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks)
        for stock in founds:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks)
        for stock in fix_income:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks)
        infos['stocks'] = stocks
        infos['stocks_br'] = stocks_br
        infos['bdrs'] = bdrs
        infos['fiis'] = fiis
        infos['founds'] = founds
        infos['fix_income'] = fix_income
        return infos

    def add_dividend_info(self, stock, headers=None):
        stock_ = generic_repository.get_object("stocks", ["ticker"], stock)
        arg = {'investment_id': stock_['id']}
        dividends = dividend_handler.get_dividends(arg, headers)
        stock['dividends'] = 0
        for dividend in dividends:
            stock['dividends'] += float(dividend['quantity'] * dividend['value_per_quote'])
        stock['dyr'] = stock['dividends'] / stock['total_value_atu']
        return stock

    def add_daily_gain(self, stock, headers=None):
        stock_ = generic_repository.get_object("stocks", ["ticker"], stock)
        filter_ = {
            'investment_id': stock_['id'],
            'user_id': generic_repository.get_user(headers)['id'],
            'movement_type': 1
        }
        movements = generic_repository.get_objects("user_stocks_movements",
                                                   ["investment_id", "user_id", "movement_type"], filter_)
        days = 0
        for movement in movements:
            delta = datetime.now() - movement['date']
            days += delta.days * movement['quantity']
        filter_['movement_type'] = 2
        movements = generic_repository.get_objects("user_stocks_movements",
                                                   ["investment_id", "user_id", "movement_type"], filter_)
        for movement in movements:
            delta = datetime.now() - movement['date']
            days -= delta.days * movement['quantity']

        avg_days = days / stock['quantity']
        if avg_days is None or avg_days < 1:
            avg_days = 1

        stock['daily_gain'] = stock['gain'] / float(avg_days)
        stock['daily_dyr'] = stock['dyr'] / float(avg_days)
        stock['daily_total_gain'] = stock['daily_dyr'] + stock['daily_gain']
        stock['monthly_gain'] = (stock['daily_total_gain'] + float(1)) ** float(30) - float(1)

        stock['total_gain'] = stock['dyr'] + stock['gain']
        return stock

    def get_ticket_info(self, ticker, stocks, atr):
        for stock in stocks:
            if stock['ticker'] == ticker:
                return stock[atr]
        return 0

    def set_stock_weight(self, stock, types_sum, stocks):
        stock['ticker_weight_in_all'] = 0
        stock['ticker_weight_in_type'] = 0
        try:
            segment_total = types_sum['segment_sum'][stock['segment']]
            stock['segment_weight_in_all'] = segment_total / types_sum[0]
            stock['segment_weight_in_type'] = segment_total / types_sum[stock['investment_type_id']]
        except:
            stock['segment_weight_in_all'] = 0
            stock['segment_weight_in_type'] = 0

        try:
            stock['type_weight'] = types_sum[stock['investment_type_id']] / types_sum[0]
        except:
            stock['type_weight'] = 0

        try:
            stock['ticker_weight_in_type'] = stock['total_value_atu'] / types_sum[stock['investment_type_id']]
        except:
            for stock_ in stocks:
                if stock_['ticker'] == stock['ticker']:
                    stock['ticker_weight_in_type'] = stock_['ticker_weight_in_type']
                    break
        try:
            stock['ticker_weight_in_all'] = stock['total_value_atu'] / types_sum[0]
        except:
            for stock_ in stocks:
                if stock_['ticker'] == stock['ticker']:
                    stock['ticker_weight_in_all'] = stock_['ticker_weight_in_all']
                    break
        return stock

    def get_ideal_value(self, value_tot, perc_atu, perc_ideal):
        if perc_atu > perc_ideal:
            valor_atu = value_tot * perc_atu
            novo_total = value_tot - (valor_atu - value_tot * (perc_ideal))
            novo_valor = value_tot * (perc_ideal)
            sell = valor_atu - novo_valor
            val_atu = novo_valor
            while sell > 0.01:
                novo_total = novo_total - (val_atu - novo_total * (perc_ideal))
                novo_valor = novo_total * (perc_ideal)
                sell = val_atu - novo_valor
                val_atu = novo_valor
            return novo_valor
        else:
            v_atu = value_tot * perc_atu
            new_tot = value_tot + (value_tot * (perc_ideal) - v_atu)
            new_val = value_tot * (perc_ideal)
            buy = new_val - v_atu
            val_atu = new_val
            while buy > 0.01:
                new_tot = new_tot + (new_tot * (perc_ideal) - val_atu)
                new_val = new_tot * (perc_ideal)
                buy = new_val - val_atu
                val_atu = new_val

            return new_val

    def set_buy_sell_info(self, stock_, stock_ref, types_sum, stocks):
        ticker_perc_max_ideal = 0.05
        great_gain = 0.07
        type_ivest_id_ = stock_['investment_type_id']
        if type_ivest_id_ == 16:
            total_invested = types_sum[0]
            perc_type_ideal = 0.50
            ticker_perc_max_ideal = 1
        elif type_ivest_id_ == 15:
            total_invested = types_sum[0]
            perc_type_ideal = 0.15
            ticker_perc_max_ideal = 1
        elif type_ivest_id_ == 2:
            total_invested = types_sum[type_ivest_id_]
            perc_type_ideal = 0.25
        else:
            total_invested = types_sum[1] + types_sum[4]
            perc_type_ideal = 0.10

        perc_refer = 'ticker_weight_in_all'
        stock_['buy_sell_indicator'] = "neutral"
        stock_ = self.set_stock_weight(stock_, types_sum, stocks)
        type_weight = stock_['type_weight']
        type_value_ideal = self.get_ideal_value(types_sum[0], type_weight, perc_type_ideal)
        try:
            max_rank_to_buy = 20 - types_sum['types_count'][type_ivest_id_]
        except:
            max_rank_to_buy = 20
        if stock_ref is not None:
            try:
                if stock_[perc_refer] is None:
                    stock_[perc_refer] = self.get_ticket_info(stock_['ticker'], stocks, perc_refer)
            except:
                stock_[perc_refer] = self.get_ticket_info(stock_['ticker'], stocks, perc_refer)
            try:
                if stock_['monthly_gain'] is None:
                    stock_['monthly_gain'] = self.get_ticket_info(stock_['ticker'], stocks, 'monthly_gain')
            except:
                stock_['monthly_gain'] = self.get_ticket_info(stock_['ticker'], stocks, 'monthly_gain')

            rank = stock_ref['rank']
            if rank <= max_rank_to_buy:
                if stock_['monthly_gain'] > great_gain:
                    stock_['buy_sell_indicator'] = "great-gain"
                    stock_['recommendation'] = "Sell all - great gain -> " + self.to_percent_from_aliq(
                        stock_['monthly_gain']) + " -> Rank: " + str(rank)
                elif stock_[perc_refer] > ticker_perc_max_ideal or type_weight > perc_type_ideal:
                    if stock_[perc_refer] > ticker_perc_max_ideal:
                        sell_rec = self.get_tot_to_sell(total_invested, ticker_perc_max_ideal, stock_[perc_refer])
                        stock_['recommendation'] = "Sell to equilibrate the ticker " + self.to_brl(sell_rec) \
                                                   + " -> Rank: " + str(rank)
                    elif type_weight > perc_type_ideal:
                        sell_rec = self.get_tot_to_sell(types_sum[0], perc_type_ideal, type_weight)
                        stock_['recommendation'] = "Sell to equilibrate the type " + self.to_brl(sell_rec) \
                                                   + " -> Rank: " + str(rank)
                else:
                    if type_weight < perc_type_ideal:
                        stock_['buy_sell_indicator'] = "buy"
                        sell_rec = self.get_tot_to_buy(types_sum[0], ticker_perc_max_ideal, stock_[perc_refer])
                        if types_sum[type_ivest_id_] + sell_rec > type_value_ideal:
                            sell_rec = sell_rec - (types_sum[type_ivest_id_] + sell_rec - type_value_ideal)
                        stock_['recommendation'] = "Max buy recommendation for the ticker " + self.to_brl(sell_rec) \
                                                   + " -> Rank: " + str(rank)

            elif rank > 40:
                stock_['buy_sell_indicator'] = "sell"
                if stock_['monthly_gain'] > great_gain:
                    stock_['buy_sell_indicator'] = "great-gain"
                    stock_['recommendation'] = "Sell all - great gain -> " + self.to_percent_from_aliq(
                        stock_['monthly_gain']) + " -> Rank: " + str(rank)
                elif stock_['ticker_weight_in_all'] > 0:
                    if rank > 10000:
                        tiker_prefix = ''.join([i for i in stock_['ticker'] if not i.isdigit()])
                        stock_['recommendation'] = "Sell all - another " + tiker_prefix + " ticker best ranked "
                    else:
                        stock_['recommendation'] = "Sell all - strategy" \
                                                   + " -> Rank: " + str(rank)
                else:
                    if rank > 10000:
                        tiker_prefix = ''.join([i for i in stock_['ticker'] if not i.isdigit()])
                        stock_['recommendation'] = "Do not buy - another " + tiker_prefix + " ticker best ranked "
                    else:
                        stock_['recommendation'] = "Do not buy" \
                                                   + " -> Rank: " + str(rank)
            else:
                if stock_['monthly_gain'] > great_gain:
                    stock_['buy_sell_indicator'] = "great-gain"
                    stock_['recommendation'] = "Sell all - great gain -> " + self.to_percent_from_aliq(
                        stock_['monthly_gain']) + " -> Rank: " + str(rank)
                elif stock_[perc_refer] > ticker_perc_max_ideal:
                    sell_rec = self.get_tot_to_sell(total_invested, ticker_perc_max_ideal, stock_[perc_refer])
                    stock_['recommendation'] = "Sell to equilibrate " + self.to_brl(sell_rec) \
                                               + " -> Rank: " + str(rank)
                else:
                    stock_['recommendation'] = "Neutral -> Rank: " + str(rank)

        else:
            if stock_['monthly_gain'] > great_gain:
                stock_['buy_sell_indicator'] = "great-gain"
                stock_['recommendation'] = "Sell all - great gain -> " + self.to_percent_from_aliq(
                    stock_['monthly_gain'])
            elif stock_[perc_refer] > ticker_perc_max_ideal:
                sell_rec = self.get_tot_to_sell(total_invested, ticker_perc_max_ideal, stock_['perc_atu'])
                stock_['recommendation'] = "Sell to equilibrate " + self.to_brl(sell_rec)
            else:
                stock_['recommendation'] = "Sell all - strategy"

    def get_stock_ref(self, bdrs, fiis, stock, stock_ref, stocks_br, founds, fix_income):
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
        elif stock['investment_type_id'] == 15:
            for obj_ in founds:
                if obj_['ticker'] == stock['ticker']:
                    stock_ref = obj_
                    break
        elif stock['investment_type_id'] == 16:
            for obj_ in fix_income:
                if obj_['ticker'] == stock['ticker']:
                    stock_ref = obj_
                    break
        return stock_ref

    def get_tot_to_sell(self, total, perc_ideal, perc_atu):
        valor_atu = total * perc_atu
        # novo_total = total - (valor_atu - total * (perc_ideal / 100))
        novo_valor = total * perc_ideal
        sell = valor_atu - novo_valor
        return sell
        # val_atu = novo_valor
        # while sell > 0.01:
        #     novo_total = novo_total - (val_atu - novo_total * (perc_ideal / 100))
        #     novo_valor = novo_total * (perc_ideal / 100)
        #     sell = val_atu - novo_valor
        #     val_atu = novo_valor
        # return valor_atu - novo_valor

    def get_tot_to_buy(self, total, perc_ideal, perc_atu):
        v_atu = total * perc_atu
        # new_tot = total + (total * (perc_ideal / 100) - v_atu)
        new_val = total * perc_ideal
        buy = new_val - v_atu
        return buy
        # val_atu = new_val
        # while buy > 0.01:
        #     new_tot = new_tot + (new_tot * (perc_ideal / 100) - val_atu)
        #     new_val = new_tot * (perc_ideal / 100)
        #     buy = new_val - val_atu
        #     val_atu = new_val
        #
        # return new_val - v_atu

    def to_brl(self, value):
        a = '{:,.2f}'.format(float(value))
        b = a.replace(',', 'v')
        c = b.replace('.', ',')
        return "R$ " + c.replace('v', '.')

    def to_percent_from_aliq(self, value):
        return self.to_percent(value * 100)

    def to_percent(self, value):
        a = '{:,.2f}'.format(float(value))
        b = a.replace(',', 'v')
        c = b.replace('.', ',')
        return c.replace('v', '.') + "%"

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

    def att_stock_price_new(self, daily, stock, stock_, type, price_type="4", reimport=False, data_=None):
        if stock_['prices_imported'] == 'N' or daily or reimport:
            if type == 'fundo' and not daily or type == 'fundo' and reimport:
                company_ = stock_['url_infos']
                company_ = company_.replace('/fundos-de-investimento/', '')
                infos = http_repository.get_prices_fundos(company_, price_type == "1")
                datas = infos['data']['chart']['category']
                values = infos['data']['chart']['series']['fundo']
                for i in range(len(datas)):
                    data = datas[i]
                    data = datetime.strptime(data, '%d/%m/%y').strftime("%Y-%m-%d")
                    can_insert = True
                    if data_ is not None:
                        can_insert = data > data_
                    price = values[i]['price']
                    stock_['price'] = price
                    if can_insert:
                        self.add_stock_price(stock_, data)
                pass
            else:
                infos = http_repository.get_prices(stock_['ticker'], type, daily, price_type)
                for info in infos:
                    prices = info['prices']
                    for price in prices:
                        stock_['price'] = price['price']
                        if daily:
                            data = datetime.strptime(price['date'], '%d/%m/%y %H:%M').strftime("%Y-%m-%d %H:%M")
                            can_insert = True
                            if data_ is not None:
                                can_insert = data > data_
                            if can_insert:
                                self.add_stock_price(stock_,
                                                     data)
                        else:
                            data = datetime.strptime(price['date'], '%d/%m/%y %H:%M').strftime("%Y-%m-%d")
                            can_insert = True
                            if data_ is not None:
                                can_insert = data > data_
                            if can_insert:
                                self.add_stock_price(stock_,
                                                     data)
            stock_['prices_imported'] = 'S'
            stock_['price'] = stock['price']
            generic_repository.update("stocks", ["ticker"], stock_)
        print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
