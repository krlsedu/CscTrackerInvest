import decimal
import json
from datetime import timedelta, datetime, timezone
from statistics import stdev, mean

import pandas as pd
import pytz
import requests

from repository.HttpRepository import HttpRepository
from service.DividendHandler import DividendHandler
from service.FiiHandler import FiiHandler
from service.FixedIncome import FixedIncome
from service.Interceptor import Interceptor
from service.RequestHandler import RequestHandler
from service.StocksHandler import StocksHandler
from service.Utils import Utils

http_repository = HttpRepository()
fii_handler = FiiHandler()
stock_handler = StocksHandler()
fixed_income_handler = FixedIncome()
dividend_handler = DividendHandler()
request_handler = RequestHandler()
utils = Utils()

url_bff = 'http://bff:8080/'


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)


class InvestmentHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def add_movements(self, movements, headers=None):
        msgs = []
        for movement in movements:
            msgs.append(self.add_movement(movement, headers))
        return msgs

    def add_movement(self, movement, headers=None):
        movement = http_repository.add_user_id(movement, headers)
        movement_type = http_repository.get_object("movement_types", ["id"], {"id": movement['movement_type']}, headers)
        coef = movement_type['coefficient']
        ticker_ = movement['ticker']
        if ticker_ is not None:
            try:
                fixed_icome = movement['fixed_icome']
            except:
                fixed_icome = "N"
            if fixed_icome != "S":
                stock = self.get_stock(ticker_, headers)
            else:
                stock = fixed_income_handler.get_stock(movement, headers)
                price = fixed_income_handler.get_stock_price(movement, headers)
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
            if http_repository.exist_by_key("user_stocks", ["investment_id"], movement, headers):
                if movement_type['to_balance']:
                    user_stock = http_repository.get_object("user_stocks", ["investment_id"], movement, headers)
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
                        self.add_profit_loss(profit_loss_value, movement, headers)
                    http_repository.update("user_stocks", ["user_id", "investment_id"], user_stock, headers)
                http_repository.insert("user_stocks_movements", movement, headers)
            else:
                if movement_type['to_balance']:
                    stock = {'investment_id': movement['investment_id'], 'quantity': movement['quantity'],
                             'avg_price': movement['price'], 'user_id': movement['user_id'],
                             'investment_type_id': movement['investment_type_id']}
                    http_repository.insert("user_stocks", stock, headers)
                http_repository.insert("user_stocks_movements", movement, headers)
            return {"status": "success", "message": "Movement added"}
        except Exception as e:
            print(e)
            return {"status": "error", "message": e}

    def add_profit_loss(self, profit_loss_value, movement, headers=None):
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
        http_repository.insert("profit_loss", profit_loss, headers)

    def get_stock(self, ticker_, headers=None):
        ticker_ = ticker_.upper()
        stock = http_repository.get_object("stocks", ["ticker"], {"ticker": ticker_}, headers)
        try:
            stock['id']
        except Exception as e:
            ticker_ = self.add_stock(ticker_, headers)
            stock = http_repository.get_object("stocks", ["ticker"], {"ticker": ticker_}, headers)
            self.add_stock_price(stock, headers)
        return stock

    def add_stock_price(self, stock, headers=None, date=None):
        stock_price = {
            "investment_id": stock['id'],
            "price": stock['price'],
        }
        if date is not None:
            stock_price['date_value'] = date

        data_ant = datetime.now().astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        try:
            price_ant = stock_handler.get_price(stock['id'], data_ant, headers)
        except:
            price_ant = None
        if price_ant is not None:
            if float(price_ant['price']) != float(stock['price']):
                self.add_price(stock_price, headers)
        else:
            self.add_price(stock_price, headers)

    def add_price(self, price, headers=None):
        try:
            if price['price'] is not None:
                http_repository.insert("stocks_prices", price, headers)
        except Exception as e:
            print(e)

    def add_stock(self, ticker_, headers=None):
        stock = http_repository.get_firt_stock_type(ticker_, headers)
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
        http_repository.insert("stocks", investment_tp, headers)
        return investment_tp['ticker']

    def get_stocks(self, args=None, headers=None):
        filters = []
        values = {}
        for key in args:
            filters.append(key)
            values[key] = args[key]
        return http_repository.get_objects("user_stocks", filters, values, headers)

    def att_prices_yahoo(self, stock_, headers, period, interval):
        try:
            prices = requests.get(url_bff +
                                  'yahoofinance/prices-br/' + stock_['ticker'] + '/' + period + '/' + interval,
                                  headers=headers).json()
            for price in prices['data']:
                try:
                    stock_['price'] = price['open']

                    self.add_stock_price(stock_, headers,
                                         datetime.strptime(price['date'], '%Y-%m-%dT%H:%M:%S.%fZ')
                                         .replace(tzinfo=timezone.utc)
                                         .strftime('%Y-%m-%d %H:%M:%S'))
                except Exception as e:
                    print(e)
                    pass
        except Exception as e:
            print(e)
            pass

    def att_price_yahoo(self, stock_, headers, date=None):
        try:
            prices = requests.get(url_bff + 'yahoofinance/price-br/' + stock_['ticker'], headers=headers) \
                .json()
            stock_['price'] = prices['price']['regularMarketPrice']
            self.add_stock_price(stock_, headers, date)
            http_repository.update("stocks", ["ticker"], stock_, headers)
        except:
            try:
                self.add_stock_price(stock_, headers, date)
                http_repository.update("stocks", ["ticker"], stock_, headers)
            except:
                pass
            pass
        return stock_

    def att_price_yahoo_us(self, stock_, headers, date=None):
        try:
            prices = requests.get(url_bff + 'yahoofinance/price/' + stock_['ticker'], headers=headers) \
                .json()
            price_ = prices['price']['regularMarketPrice']
            prices = requests.get(url_bff + 'yahoofinance/price/BRL=X', headers=headers) \
                .json()
            price_ = price_ * prices['price']['regularMarketPrice']
            stock_['price'] = price_
            self.add_stock_price(stock_, headers, date)
            http_repository.update("stocks", ["ticker"], stock_, headers)
        except:
            try:
                self.add_stock_price(stock_, headers, date)
                http_repository.update("stocks", ["ticker"], stock_, headers)
            except:
                pass
            pass

    def att_info_yahoo(self, stock_, headers, date=None):
        try:
            prices = requests.get(url_bff + 'yahoofinance/info-br/' + stock_['ticker'], headers=headers) \
                .json()

            try:
                stock_['price'] = prices['price']['regularMarketPrice']
            except Exception as e:
                print(e)
                pass
            try:
                stock_['ebitda'] = prices['financial_data']['ebitda']
            except Exception as e:
                print(e)
                stock_['ebitda'] = 0
            try:
                stock_['ev'] = prices['key_stats']['enterpriseValue']
            except Exception as e:
                print(e)
                stock_['ev'] = 0

            if stock_['ev'] is None:
                stock_['ev'] = 0
            if stock_['ebitda'] is None:
                stock_['ebitda'] = 0
            if stock_['ebitda'] != 0:
                stock_['ev_ebitda'] = stock_['ev'] / stock_['ebitda']
            if stock_['ev_ebitda'] is None:
                stock_['ev_ebitda'] = 0
            if stock_['ev_ebit'] is None:
                stock_['ev_ebit'] = 0
            self.add_stock_price(stock_, headers, date)
            http_repository.update("stocks", ["ticker"], stock_, headers)
        except Exception as e:
            print(e)
            try:
                self.add_stock_price(stock_, headers, date)
                http_repository.update("stocks", ["ticker"], stock_, headers)
            except Exception as e:
                print(e)
                pass
            pass

    def att_dividend_info(self, stock, headers):
        select = f"select value_per_quote as value " \
                 f"from dividends_map m, " \
                 f"stocks st " \
                 f"where st.id = m.investment_id " \
                 f"and st.ticker = '{stock['ticker']}' " \
                 f"and date_with >= now() + interval '-1 year' order by date_with desc"
        values = http_repository.execute_select(select, headers)
        if values.__len__() > 0:
            vs = []
            for value in values:
                vs.append(float(value['value']))
            if vs.__len__() < 12:
                vs = vs + [0] * (12 - vs.__len__())
            f = stdev(vs)
            mean1 = mean(vs)
            if mean1 == 0:
                stock["desv_dy"] = 10000000
            else:
                mean_ = f / mean1
                stock["desv_dy"] = mean_
            price_ = stock["price"]
            if price_ > 0:
                stock["dy"] = (mean1 * 12) / price_ * 100
            else:
                stock["dy"] = 0
            stock["last_dividend"] = float(values[0]['value'])
            http_repository.update("stocks", ["ticker"], stock, headers)
        else:
            stock["dy"] = 0
            stock["last_dividend"] = 0
            stock["desv_dy"] = 10000000
            http_repository.update("stocks", ["ticker"], stock, headers)
        return stock

    def get_stocks_consolidated(self, args=None, headers=None):
        stocks = self.get_stocks(args, headers)
        if stocks.__len__() > 0:
            stocks_consolidated = []
            segments = []
            for stock in stocks:
                if stock['quantity'] > 0:
                    segment = {}
                    stock_consolidated = {}
                    investment_type = http_repository.get_object("stocks", ["id"], {"id": stock['investment_id']},
                                                                 headers)
                    ticker_ = investment_type['ticker']
                    stock['ticker'] = ticker_
                    stock_ = http_repository.get_object("stocks", ["ticker"], stock, headers)
                    stock_, investment_type = http_repository.get_values_by_ticker(stock_, False, headers)
                    if investment_type['id'] == 100:
                        self.att_price_yahoo_us(stock_, headers)
                    else:
                        self.att_price_yahoo(stock_, headers)

                    if investment_type['id'] == 16:
                        stock_price = fixed_income_handler \
                            .get_stock_price_by_ticker(ticker_, headers, (datetime.now()).strftime('%Y-%m-%d'))
                        stock_['price'] = stock_price['price']
                    stock_ = self.att_dividend_info(stock_, headers)
                    data_ant = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d 23:59:59')
                    price_ant = stock_handler.get_price(stock_['id'], data_ant, headers)
                    data_atu = datetime.now().strftime('%Y-%m-%d 23:59:59')
                    price_atu = stock_handler.get_price(stock_['id'], data_atu, headers)

                    segment['type'] = investment_type['name']
                    segment['type_id'] = investment_type['id']
                    stock_consolidated['type'] = investment_type['name']

                    stock_consolidated['price_atu'] = stock_['price']
                    if price_ant is not None:
                        stock_consolidated['price_ant'] = price_ant['price']
                        stock_consolidated['price_atu'] = price_atu['price']
                        dt_prc_ant = datetime.strptime(price_ant['date_value'], '%Y-%m-%d %H:%M:%S.%f') \
                            .replace(tzinfo=timezone.utc).strftime('%Y-%m-%d')
                        dt_prc_req = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
                        if dt_prc_ant < \
                                dt_prc_req:
                            if investment_type['id'] == 15:
                                self.att_stock_price_new(headers, False, stock_, stock_, "fundo", "1", True, dt_prc_ant)
                            elif investment_type['id'] == 1:
                                self.att_stock_price_new(headers, False, stock_, stock_, "acao", "1", True, dt_prc_ant)
                            elif investment_type['id'] == 2:
                                self.att_stock_price_new(headers, False, stock_, stock_, "fii", "1", True, dt_prc_ant)
                            elif investment_type['id'] == 4:
                                self.att_stock_price_new(headers, False, stock_, stock_, "bdr", "1", True, dt_prc_ant)
                            pass

                    if stock_['segment_custom'] is not None:
                        segment['segment'] = stock_['segment_custom']
                        stock_consolidated['segment'] = stock_['segment_custom']
                    else:
                        stock_['segment_custom'] = stock_['segment']
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

                            stock_consolidated['value_ant_date'] = datetime \
                                .strptime(price_ant['date_value'], '%Y-%m-%d %H:%M:%S.%f') \
                                .replace(tzinfo=timezone.utc).strftime('%Y-%m-%d')
                            stock_consolidated['variation'] = stock_consolidated['total_value_atu'] - \
                                                              stock_consolidated['total_value_ant']
                    else:
                        stock_consolidated['total_value_atu'] = float(stock['quantity']) * \
                                                                float(stock_consolidated['price_atu'])
                        if price_ant is not None:
                            stock_consolidated['total_value_ant'] = float(stock['quantity']) * float(price_ant['price'])
                            stock_consolidated['value_ant_date'] = datetime \
                                .strptime(price_ant['date_value'], '%Y-%m-%d %H:%M:%S.%f') \
                                .replace(tzinfo=timezone.utc).strftime('%Y-%m-%d')
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
                    segment['total_value_invest'] = float(stock_consolidated['total_value_invest'])
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
        stocks_br = stock_handler.get_stocks(1, headers, args)
        bdrs = stock_handler.get_stocks(4, headers, args)
        fiis = fii_handler.get_fiis(headers, args)
        founds = stock_handler.get_founds(15, headers)
        fix_income = stock_handler.get_founds(16, headers)
        criptos = stock_handler.get_founds(100, headers)
        return stocks_br, bdrs, fiis, founds, fix_income, criptos

    def buy_sell_indication(self, args=None, headers=None):
        infos = self.get_stocks_consolidated(args, headers)
        infos['stocks_names'] = stock_handler.get_stocks_basic(headers)

        stocks = infos['stocks']
        type_grouped = infos['type_grouped']
        total_value_atu = 0
        total_value_invest = 0
        types_sum = {}
        types_count = {}
        for type_ in type_grouped:
            total_value_atu += type_['total_value_atu']
            total_value_invest += type_['total_value_invest']
            types_sum[type_['type_id']] = type_['total_value_atu']
            types_count[type_['type_id']] = 0
        segment_grouped = infos['segments_grouped']
        segments_sum = {}
        for segment in segment_grouped:
            segments_sum[segment['segment']] = segment['total_value_atu']
        types_sum[0] = total_value_atu
        types_sum['segment_sum'] = segments_sum
        types_sum['types_count'] = types_count
        infos['total_invested'] = total_value_atu
        infos['resume'] = {}
        infos['resume']['total_value_atu'] = total_value_atu
        infos['resume']['total_value_invest'] = total_value_invest
        stocks_br, bdrs, fiis, founds, fix_income, criptos = self.get_sotcks_infos(args, headers)
        stock_ref = None
        for stock in stocks:
            stock_ = stock
            types_sum['types_count'][stock['investment_type_id']] += 1
            stock_ref = None
            stock_ref = self.get_stock_ref(bdrs, fiis, stock_, stock_ref, stocks_br, founds, fix_income, criptos)
            self.set_buy_sell_info(stock_, stock_ref, types_sum, stocks, headers, True)
        for stock in stocks_br:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks, headers)
        for stock in bdrs:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks, headers)
        for stock in fiis:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks, headers)
        for stock in founds:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks, headers)
        for stock in fix_income:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks, headers)
        for stock in criptos:
            stock_ref = stock
            self.set_buy_sell_info(stock, stock_ref, types_sum, stocks, headers)
        infos['stocks'] = stocks
        infos['stocks_br'] = stocks_br
        infos['bdrs'] = bdrs
        infos['fiis'] = fiis
        infos['founds'] = founds
        infos['fix_income'] = fix_income
        infos['criptos'] = criptos
        infos['types_count'] = types_sum['types_count']
        infos = self.add_total_dividends_info(infos, headers)
        infos = self.add_total_profit_loss_info(infos, headers, args)
        infos = self.add_total_daily_gain(infos, headers)
        self.save_type_gruped(infos, headers)
        self.save_resume(infos, headers)
        self.save_infos(infos, headers)
        self.save_recomendations_info(infos, headers)
        return infos

    def save_recomendations_info(self, info, headers=None):
        try:
            _ = http_repository.get_all_objects("user_recomendations", headers)
            if len(_) > 0:
                http_repository.delete_all("user_recomendations", headers)
            self.save_recomendations(info['stocks_br'], headers)
            self.save_recomendations(info['bdrs'], headers)
            self.save_recomendations(info['fiis'], headers)
            self.save_recomendations(info['founds'], headers)
            self.save_recomendations(info['fix_income'], headers)
            self.save_recomendations(info['criptos'], headers)
        except Exception as e:
            print(e)
            pass

    def save_recomendations(self, recomendations, headers=None):
        for recomendation in recomendations:
            recomendation["investment_id"] = \
                http_repository.get_object("stocks", ["ticker"], recomendation, headers)['id']
            try:
                del recomendation['name']
            except:
                pass
            try:
                del recomendation['url_infos']
            except:
                pass
            try:
                del recomendation['url_fundamentos']
            except:
                pass
            try:
                del recomendation['url_statusinvest']
            except:
                pass
            try:
                http_repository.insert("user_recomendations", recomendation, headers)
            except Exception as e:
                print(e)
                pass

    def save_type_gruped(self, infos, headers=None):
        type_grouped = infos['type_grouped']
        for type_ in type_grouped:
            type_['id'] = type_['type_id']
            http_repository.update("type_gruped_values", ["id"], type_, headers)

    def save_resume(self, infos, headers=None):
        resume = infos['resume']
        resume['id'] = 9999
        resume['type_id'] = 9999
        http_repository.update("resume_values", ["id"], resume, headers)

    def save_infos(self, infos, headers=None):
        resume = None
        try:
            resume = http_repository.get_object("investments_calc_resume", [], {}, headers)
        except:
            pass
        j_infos = json.dumps(infos, cls=Encoder, ensure_ascii=False)
        if resume is None:
            resume = {'resume': j_infos}
            http_repository.insert("investments_calc_resume", resume, headers)
        else:
            resume['resume'] = j_infos
            http_repository.update("investments_calc_resume", ["id"], resume, headers)

    def add_dividend_info(self, stock, headers=None):
        stock_ = http_repository.get_object("stocks", ["ticker"], stock, headers)
        arg = {
            'investment_id': stock_['id'],
            'active': 'S'
        }
        dividends = dividend_handler.get_dividends(arg, headers)
        stock['dividends'] = 0
        for dividend in dividends:
            stock['dividends'] += float(dividend['quantity'] * dividend['value_per_quote'])
        stock['dyr'] = stock['dividends'] / stock['total_value_atu']
        return stock

    def add_total_dividends_info(self, infos, headers=None):
        arg = {}
        dividends = dividend_handler.get_dividends(arg, headers)
        infos['resume']['total_dividends'] = 0
        for dividend in dividends:
            infos['resume']['total_dividends'] += float(dividend['quantity'] * dividend['value_per_quote'])
        infos['resume']['dyr'] = infos['resume']['total_dividends'] / infos['resume']['total_value_atu']
        return infos

    def add_total_profit_loss_info(self, infos, headers=None, args=None):
        filters, values = http_repository.get_filters(args, headers)
        profit_loss = http_repository.get_objects("profit_loss", filters, values, headers)
        infos['resume']['profits'] = 0
        infos['resume']['losses'] = 0
        for pl in profit_loss:
            if pl['value'] > 0:
                infos['resume']['profits'] += float(pl['value'] * pl['quantity'])
            else:
                infos['resume']['losses'] += float(pl['value'] * pl['quantity'])
        return infos

    def add_total_daily_gain(self, infos, headers=None):
        filter_ = {
            'user_id': http_repository.get_user(headers)['id'],
            'movement_type': 1
        }
        movements = http_repository.get_objects("user_stocks_movements",
                                                ["user_id", "movement_type"], filter_, headers)
        days = 0
        quantity = 0
        for movement in movements:
            date_ = datetime.strptime(movement['date'], '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)
            delta = datetime.now().astimezone(tz=timezone.utc) - date_
            days += delta.days * (movement['quantity'] * movement['price'])
            quantity += (movement['quantity'] * movement['price'])
        filter_['movement_type'] = 2
        movements = http_repository.get_objects("user_stocks_movements",
                                                ["user_id", "movement_type"], filter_, headers)
        for movement in movements:
            date_ = datetime.strptime(movement['date'], '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)
            delta = datetime.now().astimezone(tz=timezone.utc) - date_
            days -= delta.days * (movement['quantity'] * movement['price'])
            quantity -= (movement['quantity'] * movement['price'])

        avg_days = days / quantity
        if avg_days is None or avg_days < 1:
            avg_days = 1
        infos['resume']['gain'] = infos['resume']['total_value_atu'] / infos['resume']['total_value_invest'] - 1
        infos['resume']['daily_gain'] = infos['resume']['gain'] / float(avg_days)
        infos['resume']['daily_dyr'] = infos['resume']['dyr'] / float(avg_days)
        profits_r = (infos['resume']['profits'] - infos['resume']['losses']) / infos['resume']['total_value_atu']
        infos['resume']['daily_flr'] = profits_r / float(avg_days)
        infos['resume']['daily_total_gain'] = infos['resume']['daily_dyr'] + \
                                              infos['resume']['daily_gain'] + \
                                              infos['resume']['daily_flr']
        infos['resume']['monthly_gain'] = (infos['resume']['daily_total_gain'] + float(1)) ** float(30) - float(1)

        infos['resume']['total_gain'] = infos['resume']['dyr'] + infos['resume']['gain']
        return infos

    def add_daily_gain(self, stock, headers=None):
        stock_ = http_repository.get_object("stocks", ["ticker"], stock, headers)
        filter_ = {
            'investment_id': stock_['id'],
            'user_id': http_repository.get_user(headers)['id'],
            'movement_type': 1,
            'active': 'S'
        }
        movements = http_repository.get_objects("user_stocks_movements",
                                                ["investment_id", "user_id", "movement_type", "active"],
                                                filter_, headers)
        days = 0
        for movement in movements:
            date_ = datetime.strptime(movement['date'], '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)
            delta = datetime.now().astimezone(tz=timezone.utc) - date_
            days += delta.days * movement['quantity']
        filter_['movement_type'] = 2
        movements = http_repository.get_objects("user_stocks_movements",
                                                ["investment_id", "user_id", "movement_type", "active"],
                                                filter_, headers)
        for movement in movements:
            date_ = datetime.strptime(movement['date'], '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)
            delta = datetime.now().astimezone(tz=timezone.utc) - date_
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

    def check_notify_stock(self, stock, headers=None):
        stock_ = http_repository.get_object("stocks", ["ticker"], stock, headers)
        filter_ = {
            'investment_id': stock_['id'],
        }
        stock_notification_config = http_repository \
            .get_object("stock_notification_config", ["investment_id"], filter_, headers)
        if stock_notification_config is None:
            return None
        else:
            if not (utils.work_day() and utils.work_time()):
                return stock_notification_config
            if stock_notification_config['monthly_gain_target'] is not None and \
                    stock_notification_config['monthly_gain_target'] / 100 < stock['monthly_gain'] and \
                    self.check_time_to_notfy(stock_notification_config, headers):
                message = 'A ação ' + stock['ticker'] + ' atingiu o ganho mensal esperado de ' + \
                          str(stock_notification_config['monthly_gain_target']) + '%. Ganho atual: ' + \
                          str(stock['monthly_gain'] * 100) + '%'
                request_handler.inform_to_client(stock, "buySellRecommendation", headers, message,
                                                 "Buy Sell Recommendation")
                stock_notification_config['last_notification'] = datetime \
                    .strftime(datetime.now(pytz.utc), '%Y-%m-%d %H:%M:%S.%f')
                http_repository.update("stock_notification_config", ["id"], stock_notification_config, headers)
            elif stock_notification_config['value_target'] is not None and \
                    stock_notification_config['value_target'] < stock['price_atu'] and \
                    self.check_time_to_notfy(stock_notification_config, headers):
                message = 'A ação ' + stock['ticker'] + ' atingiu o valor desejado de ' + \
                          str(stock_notification_config['value_target']) + '. Valor atual: ' + \
                          str(stock['price_atu'])
                request_handler.inform_to_client(stock, "buySellRecommendation", headers, message,
                                                 "Buy Sell Recommendation")
                stock_notification_config['last_notification'] = datetime \
                    .strftime(datetime.now(pytz.utc), '%Y-%m-%d %H:%M:%S.%f')
                http_repository.update("stock_notification_config", ["id"], stock_notification_config, headers)
            return stock_notification_config

    def check_time_to_notfy(self, stock_notification_config, headers=None):
        try:
            last_update = datetime.strptime(stock_notification_config['last_notification'], '%Y-%m-%d %H:%M:%S.%f') \
                .replace(tzinfo=timezone.utc)
            astimezone = datetime.now().astimezone(timezone.utc)
            time = astimezone.timestamp() * 1000 - last_update.timestamp() * 1000
            if stock_notification_config['frequency_unit'] == 'second':
                return time > stock_notification_config['frequency'] * 1000
            elif stock_notification_config['frequency_unit'] == 'minute':
                return time > stock_notification_config['frequency'] * 1000 * 60
            elif stock_notification_config['frequency_unit'] == 'hour':
                return time > stock_notification_config['frequency_value'] * 3600000
            elif stock_notification_config['frequency_unit'] == 'day':
                return time > stock_notification_config['frequency_value'] * 86400000
            elif stock_notification_config['frequency_unit'] == 'week':
                return time > stock_notification_config['frequency_value'] * 604800000
        except Exception as e:
            print(e)
            return True

    def set_buy_sell_info(self, stock_, stock_ref, types_sum, stocks, headers=None, notify=False):
        ticker_perc_max_ideal = 0.05
        great_gain = 0.07
        great_gain_ = great_gain
        type_ivest_id_ = stock_['investment_type_id']
        total_invested = types_sum[type_ivest_id_]
        filter_ = {
            'investment_type_id': type_ivest_id_
        }
        perc_type_ideal_obj = http_repository.get_object("perc_ideal_investment_type", ["investment_type_id"],
                                                         filter_, headers)
        perc_type_ideal = perc_type_ideal_obj['perc_ideal'] / 100
        if type_ivest_id_ == 16:
            total_invested = types_sum[0]
            ticker_perc_max_ideal = 1

        perc_refer = 'ticker_weight_in_all'
        stock_['buy_sell_indicator'] = "neutral"
        stock_ = self.set_stock_weight(stock_, types_sum, stocks)
        type_weight = stock_['type_weight']
        type_value_ideal = self.get_ideal_value(types_sum[0], type_weight, perc_type_ideal)
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
            if notify:
                conf_not = self.check_notify_stock(stock_, headers)
                if conf_not is not None:
                    great_gain = conf_not['monthly_gain_target'] / 100
                else:
                    great_gain = great_gain_
            else:
                great_gain = great_gain_
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
                stock_['buy_sell_indicator'] = "sell"
                stock_['recommendation'] = "Sell all - strategy"

    def get_stock_ref(self, bdrs, fiis, stock, stock_ref, stocks_br, founds, fix_income, criptos):
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
        elif stock['investment_type_id'] == 100:
            for obj_ in criptos:
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
        df['rank_ev_ebit'] = df['ev_ebit'].rank(ascending=True)
        df['rank_ev_ebitda'] = df['ev_ebitda'].rank(ascending=True)

        return json.loads(df.to_json(orient="records"))

    def att_stocks_ranks(self, headers=None):
        stocks = http_repository.get_objects("stocks", [], {}, headers)
        stocks = self.calc_ranks(stocks)
        http_repository.update("stocks", [], stocks, headers)

    def att_stock_price_new(self, headers, daily, stock, stock_, type, price_type="4", reimport=False, data_=None):
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
                        self.add_stock_price(stock_, headers, data)
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
                                self.add_stock_price(stock_, headers, data)
                        else:
                            data = datetime.strptime(price['date'], '%d/%m/%y %H:%M').strftime("%Y-%m-%d")
                            can_insert = True
                            if data_ is not None:
                                can_insert = data > data_
                            if can_insert:
                                self.add_stock_price(stock_, headers, data)
            stock_['prices_imported'] = 'S'
            stock_['price'] = stock['price']
            http_repository.update("stocks", ["ticker"], stock_, headers)
        print(f"{stock_['ticker']} - {stock_['name']} - atualizado")

    def investment_fact(self, fact, headers=None):
        if fact['id'] is None:
            stock_ = http_repository.get_object("stocks", ["ticker"], [fact['ticker']])
            fact['investment_id'] = stock_['id']
            del fact['ticker']
            http_repository.insert("hist_user_invest_facts", fact, headers)
        else:
            http_repository.update("hist_user_invest_facts", ["id"], fact, headers)

        user_invest_configs = http_repository.get_object("user_invest_configs", ["investment_id"],
                                                         [fact['investment_id']])
        if user_invest_configs is None:
            user_invest_configs = {
                "investment_id": fact['investment_id'],
                "coef": 1
            }
            http_repository.insert("user_invest_configs", user_invest_configs, headers)

        weight = fact['weight']
        if weight is None:
            weight = 10
        if fact['yes_no'] == 'no':
            weight = weight * -1
        fact['weight'] = weight
        user_invest_configs['coef'] = user_invest_configs['coef'] + (weight / 100)
        http_repository.update("user_invest_configs", ["investment_id"], user_invest_configs, headers)
        return user_invest_configs

    def investment_calc(self, data, headers=None):
        user_invest_apply = {'amount': data['amount']}
        http_repository.insert("user_invest_apply", user_invest_apply, headers)
        user_ = http_repository.get_object("users", [], [], headers)
        select = f"select * from user_invest_apply where user_id = {user_['id']}" \
                 f" order by apply_date desc limit 1"
        user_invest_apply = http_repository.execute_select(select, headers)[0]
        perc_ideal_investment_types = http_repository.get_all_objects("perc_ideal_investment_type", headers)
        amount_types = []
        resume = http_repository.get_object("resume_values", [], {}, headers)
        total_amount = 0
        for investment_type in perc_ideal_investment_types:
            perc_ideal = investment_type['perc_ideal'] / 100
            type_gruped_values = http_repository.get_object("type_gruped_values", ["type_id"],
                                                            {"type_id": investment_type['investment_type_id']}, headers)

            amount_atu = type_gruped_values['total_value_atu']
            amount = perc_ideal * resume['total_value_atu']
            amount = amount - amount_atu
            if amount > 100:
                total_amount = total_amount + amount
                amount_type = {'investment_type_id': investment_type['investment_type_id'], 'amount': amount,
                               'user_invest_apply_id': user_invest_apply['id']}
                amount_types.append(amount_type)
        for amount_type in amount_types:
            amount = amount_type['amount']
            if amount > 100:
                amount_type['amount'] = data['amount'] * (amount / total_amount)
                http_repository.insert("user_invest_apply_type", amount_type, headers)

            select = f"select * from user_recomendations where user_id = {user_['id']} and investment_type_id = " \
                     f"{amount_type['investment_type_id']} and rank <= 20 order by rank"
            user_recomendations = http_repository.execute_select(select, headers)
            total_amount_stock = 0
            amount_stocks = []
            for user_recomendation in user_recomendations:
                amount, stock_ = self.get_amount_value(user_recomendation, resume, headers)
                if amount > stock_['price']:
                    total_amount_stock = total_amount_stock + amount
                    amount_stocks.append({'investment_id': user_recomendation['investment_id'], 'amount': amount,
                                          'user_invest_apply_id': user_invest_apply['id']})
            amount_stocks_ajust = []
            amount_ajsut = 0
            for amount_stock in amount_stocks:
                stock_ = \
                    http_repository.get_object("stocks", ["id"], {"id": amount_stock['investment_id']}, headers)
                amount = amount_stock['amount']
                if amount > stock_['price']:
                    amount_stock['amount'] = amount_type['amount'] * (amount / total_amount_stock)
                    if amount_stock['amount'] > stock_['price']:
                        num_quotas = int(amount_stock['amount'] / stock_['price'])
                        amount_stock['amount'] = num_quotas * stock_['price']
                        amount_stock['num_quotas'] = num_quotas
                        amount_ajsut = amount_ajsut + amount_stock['amount']
                        amount_stocks_ajust.append(amount_stock)
            amount_stock_temp = 0
            if amount_ajsut > 0:
                for ajsut in amount_stocks_ajust:
                    stock_ = \
                        http_repository.get_object("stocks", ["id"], {"id": ajsut['investment_id']}, headers)
                    ajsut['amount'] = amount_type['amount'] * (ajsut['amount'] / amount_ajsut)
                    num_quotas = int(ajsut['amount'] / stock_['price'])
                    ajsut['amount'] = num_quotas * stock_['price']
                    ajsut['num_quotas'] = num_quotas
                    amount_stock_temp = amount_stock_temp + ajsut['amount']
                    http_repository.insert("user_invest_apply_stock", ajsut, headers)
            resto = amount_type['amount'] - amount_stock_temp
            continuar = True
            while continuar and resto > 0:
                for user_recomendation in user_recomendations:
                    resto_temp = resto
                    if user_recomendation['rank'] <= 20:
                        amount, stock_ = self.get_amount_value(user_recomendation, resume, headers)
                        if resto > stock_['price'] and amount > 0:
                            user_invest_apply_stock = \
                                http_repository.get_object("user_invest_apply_stock",
                                                           ["investment_id", "user_invest_apply_id"],
                                                           {"investment_id": user_recomendation['investment_id'],
                                                            "user_invest_apply_id": user_invest_apply['id']}, headers)
                            if user_invest_apply_stock is None:
                                amount_stock = {'investment_id': user_recomendation['investment_id'],
                                                'amount': stock_['price'],
                                                'num_quotas': 1,
                                                'user_invest_apply_id': user_invest_apply['id']}
                                http_repository.insert("user_invest_apply_stock", amount_stock, headers)
                            else:
                                user_invest_apply_stock['amount'] = user_invest_apply_stock['amount'] + stock_['price']
                                user_invest_apply_stock['num_quotas'] = user_invest_apply_stock['num_quotas'] + 1
                                http_repository.update("user_invest_apply_stock", ["id"], user_invest_apply_stock,
                                                       headers)
                            resto = resto - stock_['price']
                if resto_temp == resto:
                    continuar = False
                    print(resto)
                    break
        self.get_sell_sugestions(user_invest_apply, headers)
        return http_repository.get_objects("user_invest_apply_stock",
                                           ['user_invest_apply_id'], {'user_invest_apply_id': user_invest_apply['id']},
                                           headers)

    def get_amount_value(self, user_recomendation, resume, headers):
        perc_ideal_investment_type = \
            http_repository.get_object("perc_ideal_investment_type", ["investment_type_id"],
                                       user_recomendation, headers)
        perc_ideal = perc_ideal_investment_type['perc_ideal'] / 100 / 20
        user_invest_config = \
            http_repository.get_object("user_invest_configs", ["investment_id"], user_recomendation, headers)
        perc_ideal = perc_ideal * user_invest_config['coef']
        stock_ = \
            http_repository.get_object("stocks", ["id"], {"id": user_recomendation['investment_id']}, headers)
        user_stock = \
            http_repository.get_object("user_stocks", ["investment_id"], user_recomendation, headers)
        if stock_['segment_custom'] is not None:
            segment_ = stock_['segment_custom']
        else:
            segment_ = stock_["segment"]
        user_segments_configs = \
            http_repository.get_object("user_segments_configs", ["segment_name"], {"segment_name": segment_},
                                       headers)
        if user_stock is not None:
            amount_atu = user_stock['quantity'] * stock_['price']
        else:
            amount_atu = 0
        amount = perc_ideal * resume['total_value_atu']
        amount = amount - amount_atu
        if user_segments_configs is not None:
            if user_segments_configs['ideal_prec_max'] <= user_recomendation['segment_weight_in_all'] * 100:
                amount = 0
        return amount, stock_

    def get_sell_sugestions(self, user_invest_apply, headers=None):

        user_stocks = http_repository.get_all_objects("user_stocks", headers)
        user_stocks = [user_stock for user_stock in user_stocks if user_stock['quantity'] > 0]
        for user_stock in user_stocks:
            stock_ = \
                http_repository.get_object("stocks", ["id"], {"id": user_stock['investment_id']}, headers)
            user_recommendation = \
                http_repository.get_object("user_recomendations", ["investment_id"], user_stock, headers)

            user_invest_sell_stock = {
                "user_invest_apply_id": user_invest_apply['id'],
                "investment_id": user_stock['investment_id'],
                "amount": user_stock['quantity'] * stock_['price'],
                "num_quotas": user_stock['quantity'],
                "avg_value_quota": stock_['price'],
                "avg_price_quota": user_stock['avg_price'],
            }
            if user_recommendation is not None:
                if user_recommendation['buy_sell_indicator'] == 'sell' or \
                        user_recommendation['buy_sell_indicator'] == 'great-gain':
                    if user_recommendation['rank'] > 20:
                        user_invest_sell_stock["motive"] = \
                            user_recommendation['buy_sell_indicator'] + " - " + str(user_recommendation['rank'])
                        http_repository.insert("user_invest_sell_stock", user_invest_sell_stock, headers)
            else:
                user_invest_sell_stock["motive"] = "not recommended"
                http_repository.insert("user_invest_sell_stock", user_invest_sell_stock, headers)

    def last_investment_calc(self, headers=None):
        user_ = http_repository.get_object("users", [], [], headers)
        select = f"select * from user_invest_apply where user_id = {user_['id']}" \
                 f" order by apply_date desc limit 1"
        user_invest_apply = http_repository.execute_select(select, headers)[0]
        return self.investment_calc_info(user_invest_apply, headers)

    def investment_calc_info(self, user_invest_apply, headers):
        recomendations = {"buy_rec": http_repository.get_objects("user_invest_apply_stock",
                                                                 ['user_invest_apply_id'],
                                                                 {'user_invest_apply_id': user_invest_apply['id']},
                                                                 headers),
                          "sell_rec": http_repository.get_objects("user_invest_sell_stock",
                                                                  ['user_invest_apply_id'],
                                                                  {'user_invest_apply_id': user_invest_apply['id']},
                                                                  headers),
                          "apply_info": user_invest_apply,
                          "aplly_type": http_repository.get_objects("user_invest_apply_type",
                                                                    ['user_invest_apply_id'],
                                                                    {'user_invest_apply_id': user_invest_apply['id']},
                                                                    headers)
                          }
        return recomendations

    def save_aplly_stock(self, apply_stock, headers):
        user_invest_apply_stock = http_repository.get_object("user_invest_apply_stock", ["id"], apply_stock, headers)
        if user_invest_apply_stock is not None and user_invest_apply_stock['invested'] == 'N':
            user_invest_apply_stock['num_quotas_invested'] = apply_stock['num_quotas_invested']
            user_invest_apply_stock['avg_value_quota_invested'] = apply_stock['avg_value_quota_invested']
            user_invest_apply_stock['invested'] = 'S'
            http_repository.update("user_invest_apply_stock", ["id"], user_invest_apply_stock, headers)
            user_invest_apply = http_repository.get_object("user_invest_apply", ["id"],
                                                           {"id": user_invest_apply_stock['user_invest_apply_id']},
                                                           headers)
            user_invest_apply['value_invested'] = user_invest_apply['value_invested'] + \
                                                  (user_invest_apply_stock['num_quotas_invested'] *
                                                   user_invest_apply_stock['avg_value_quota_invested'])
            http_repository.update("user_invest_apply", ["id"], user_invest_apply, headers)
            movement = {
                "movement_type": 1,
                "ticker": apply_stock['ticker'],
                "quantity": apply_stock['num_quotas_invested'],
                "price": apply_stock['avg_value_quota_invested']
            }
            if apply_stock['num_quotas_invested'] > 0 and apply_stock['avg_value_quota_invested'] > 0:
                self.add_movement(movement, headers)
            return apply_stock

        raise Exception("Não foi possível salvar a aplicação")

    def save_aplly_stock_sell(self, apply_stock, headers):
        user_invest_sell_stock = http_repository.get_object("user_invest_sell_stock", ["id"], apply_stock, headers)
        if user_invest_sell_stock is not None and user_invest_sell_stock['executed'] == 'N':
            user_invest_sell_stock['num_quotas'] = apply_stock['num_quotas']
            user_invest_sell_stock['avg_price_quota'] = apply_stock['avg_price_quota']
            user_invest_sell_stock['executed'] = 'S'
            http_repository.update("user_invest_sell_stock", ["id"], user_invest_sell_stock, headers)
            movement = {
                "movement_type": 2,
                "ticker": apply_stock['ticker'],
                "quantity": apply_stock['num_quotas'],
                "price": apply_stock['avg_price_quota']
            }
            if apply_stock['num_quotas'] > 0 and apply_stock['avg_price_quota'] > 0:
                self.add_movement(movement, headers)
            return apply_stock

        raise Exception("Não foi possível salvar a venda")
