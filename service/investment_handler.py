import decimal
import json
import logging
from datetime import timedelta, datetime, timezone
from statistics import stdev, mean

import pandas
import pandas as pd
import pytz
from csctracker_py_core.models.emuns.config import Config
from csctracker_py_core.repository.http_repository import HttpRepository
from csctracker_py_core.repository.remote_repository import RemoteRepository
from csctracker_py_core.utils.configs import Configs
from csctracker_py_core.utils.utils import Utils

from service.dividend_handler import DividendHandler
from service.fii_handler import FiiHandler
from service.fixed_income import FixedIncome
from service.stocks_handler import StocksHandler


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)


class InvestmentHandler:
    def __init__(self,
                 fii_handler: FiiHandler,
                 stock_handler: StocksHandler,
                 fixed_income_handler: FixedIncome,
                 dividend_handler: DividendHandler,
                 remote_repository: RemoteRepository,
                 http_repository: HttpRepository):
        self.logger = logging.getLogger()
        self.fii_handler = fii_handler
        self.stock_handler = stock_handler
        self.fixed_income_handler = fixed_income_handler
        self.dividend_handler = dividend_handler
        self.remote_repository = remote_repository
        self.http_repository = http_repository
        pass

    def add_movements(self, movements, headers=None):
        msgs = []
        for movement in movements:
            msgs.append(self.add_movement(movement, headers))
        return msgs

    def add_movement(self, movement, headers=None):
        movement = self.remote_repository.add_user_id(movement, headers)
        movement_type = self.remote_repository.get_object("movement_types", ["id"], {"id": movement['movement_type']},
                                                          headers)
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
                stock = self.fixed_income_handler.get_stock(movement, headers)
                movement['ticker'] = stock['ticker']
                try:
                    movement['buy_date']
                except:
                    movement['buy_date'] = datetime.now().strftime('%Y-%m-%d')
                price = self.fixed_income_handler.get_stock_price(movement, headers)
                movement['price'] = float(price['price'])
                movement['date'] = movement['buy_date']
                try:
                    movement['quantity'] = float(movement['quantity']) / movement['price']
                except:
                    filter_ = {
                        "investment_id": stock['id']
                    }
                    user_stock = self.remote_repository.get_object("user_stocks", data=filter_, headers=headers)
                    movement['quantity'] = float(user_stock['quantity'])
            try:
                del movement['fixed_icome']
            except:
                pass
            movement['investment_id'] = stock['id']
            movement['investment_type_id'] = stock['investment_type_id']
            del movement['ticker']
        try:
            if self.remote_repository.exist_by_key("user_stocks", ["investment_id"], movement, headers):
                if movement_type['to_balance']:
                    user_stock = self.remote_repository.get_object("user_stocks", ["investment_id"], movement, headers)
                    total_value = float(user_stock['quantity'] * user_stock['avg_price'])

                    total_value += movement['quantity'] * movement['price'] * float(coef)
                    quantity = float(user_stock['quantity']) + float(movement['quantity']) * float(coef)
                    if quantity > 0:
                        if movement['movement_type'] == 2:
                            avg_price = user_stock['avg_price']
                        else:
                            avg_price = total_value / float(quantity)
                    else:
                        quantity = 0
                        avg_price = user_stock['avg_price']
                    user_stock['quantity'] = quantity
                    profit_loss_value = float(movement['price']) - float(user_stock['avg_price'])
                    user_stock['avg_price'] = avg_price
                    try:
                        user_stock['venc_date'] = movement['venc_date']
                        del movement['venc_date']
                    except:
                        pass
                    if movement['movement_type'] == 2:
                        self.add_profit_loss(profit_loss_value, movement, headers)
                    self.remote_repository.update("user_stocks", ["user_id", "investment_id"], user_stock, headers)
                try:
                    del movement['tx_type']
                except:
                    pass
                self.remote_repository.insert("user_stocks_movements", movement, headers)
            else:
                if movement_type['to_balance']:
                    stock = {'investment_id': movement['investment_id'], 'quantity': movement['quantity'],
                             'avg_price': movement['price'], 'user_id': movement['user_id'],
                             'investment_type_id': movement['investment_type_id']}
                    try:
                        stock['venc_date'] = movement['venc_date']
                        del movement['venc_date']
                    except:
                        pass
                    self.remote_repository.insert("user_stocks", stock, headers)
                try:
                    del movement['tx_type']
                except:
                    pass
                self.remote_repository.insert("user_stocks_movements", movement, headers)
            try:
                msg_ = "Movimento adicionado com sucesso: " + str(movement)
                Utils.inform_to_client(movement, "Movimento", headers, msg_)
            except Exception as e:
                self.logger.exception(e)
                pass
            return {"status": "success", "message": "Movement added"}
        except Exception as e:
            self.logger.exception(e)
            try:
                msg_ = "Erro ao adicionar movimento: " + str(e) + " - " + str(movement)
                Utils.inform_to_client(movement, "MovimentoErro", headers, msg_)
            except Exception as e:
                self.logger.exception(e)
                pass
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
        self.remote_repository.insert("profit_loss", profit_loss, headers)

    def get_stock(self, ticker_, headers=None):
        ticker_ = ticker_.upper()
        stock = self.remote_repository.get_object("stocks", ["ticker"], {"ticker": ticker_}, headers)
        try:
            stock['id']
        except Exception as e:
            ticker_ = self.add_stock(ticker_, headers)
            stock = self.remote_repository.get_object("stocks", ["ticker"], {"ticker": ticker_}, headers)
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
            price_ant = self.stock_handler.get_price(stock['id'], data_ant, headers)
        except:
            price_ant = None
        if price_ant is not None:
            if float(price_ant['price']) != float(stock['price']):
                self.add_price(stock_price, headers)
        else:
            self.add_price(stock_price, headers)

    def add_price(self, price, headers=None):
        try:
            if price['price'] is not None and price['price'] != 0:
                self.remote_repository.insert("stocks_prices", price, headers)
                try:
                    try:
                        date_value_ = datetime.strptime(price['date_value'], '%Y-%m-%d').strftime('%Y-%m-%d')
                    except:
                        date_value_ = datetime.now().strftime('%Y-%m-%d')
                    _filter = {
                        "investment_id": price['investment_id'],
                        "date_value": date_value_
                    }
                    price_agg = self.remote_repository.get_object("stocks_prices_agregated",
                                                                  data=_filter,
                                                                  headers=headers)
                    if price_agg is not None and price_agg['id'] is not None:
                        price_agg['price'] = price['price']
                        self.remote_repository.update("stocks_prices_agregated", ["id"], price_agg, headers)
                    else:
                        price['date_value'] = date_value_
                        self.remote_repository.insert("stocks_prices_agregated", price, headers)
                except Exception as e:
                    self.logger.info(f"add_price - investmentHandler -> {price}")
                    self.logger.exception(e)
                    pass
        except Exception as e:
            self.logger.exception(e)

    def add_stock(self, ticker_, headers=None):
        stock = self.http_repository.get_firt_stock_type(ticker_, headers)
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
        self.remote_repository.insert("stocks", investment_tp, headers)
        return investment_tp['ticker']

    def get_stocks(self, args=None, headers=None):
        filters = []
        values = {}
        for key in args:
            filters.append(key)
            values[key] = args[key]
        return self.remote_repository.get_objects("user_stocks", filters, values, headers)

    def att_prices_yahoo(self, stock_, headers, period, interval):
        try:
            url_bff_ = self.get_url_bff()
            prices = self.http_repository.get(
                f"{url_bff_}yahoofinance/prices-br/{stock_['ticker']}/{period}/{interval}",
                headers=headers
            ).json()
            for price in prices['data']:
                try:
                    stock_['price'] = price['open']

                    self.add_stock_price(stock_, headers,
                                         datetime.strptime(price['date'], '%Y-%m-%dT%H:%M:%S.%fZ')
                                         .replace(tzinfo=timezone.utc)
                                         .strftime('%Y-%m-%d %H:%M:%S'))
                except Exception as e:
                    self.logger.exception(e)
                    pass
        except Exception as e:
            self.logger.exception(e)
            pass

    def get_url_bff(self):
        url_bff_ = Configs.get_env_variable(Config.URL_BFF)
        if url_bff_[-1] != '/':
            url_bff_ += '/'
        return url_bff_

    def att_price_yahoo(self, stock_, headers, date=None):
        try:
            prices = self.http_repository.get(
                self.get_url_bff() + 'yahoofinance/price-br/' + stock_['ticker'],
                headers=headers
            ).json()
            stock_['price'] = prices['price']['regularMarketPrice']
            self.add_stock_price(stock_, headers, date)
            self.remote_repository.update("stocks", ["ticker"], stock_, headers)
        except:
            try:
                self.add_stock_price(stock_, headers, date)
                self.remote_repository.update("stocks", ["ticker"], stock_, headers)
            except:
                pass
            pass
        return stock_

    def att_prices_list_yahoo(self, stocks_, headers, date=None):
        range_ = 50
        stocks_blocks = [stocks_[i:i + range_] for i in range(0, len(stocks_), range_)]

        for stocks in stocks_blocks:
            try:
                tickers = ','.join(stocks)
                prices = self.http_repository.get(
                    self.get_url_bff() + f"yahoofinance/price-br?tickers={tickers}",
                    headers=headers
                ).json()
                for key in prices.keys():
                    try:
                        key_ = key.replace(".SA", "")
                        stock_ = self.remote_repository.get_object("stocks", data={"ticker": key_}, headers=headers)
                        stock_['price'] = prices[key]['regularMarketPrice']
                        self.add_stock_price(stock_, headers, date)
                        self.remote_repository.update("stocks", ["ticker"], stock_, headers)
                    except Exception as e:
                        self.logger.error(f"Erro ao atualizar o preço de {key} -> {e}")
            except Exception as e:
                self.logger.error(f"Erro ao consultar os tickers: {tickers} -> {e}")
                pass

    def att_price_yahoo_us(self, stock_, headers, date=None):
        try:
            prices = self.http_repository.get(
                self.get_url_bff() + 'yahoofinance/price/' + stock_['ticker'],
                headers=headers
            ).json()
            price_ = prices['price']['regularMarketPrice']
            prices = self.http_repository.get(
                self.get_url_bff() + 'yahoofinance/price/BRL=X',
                headers=headers
            ).json()
            price_ = price_ * prices['price']['regularMarketPrice']
            stock_['price'] = price_
            self.add_stock_price(stock_, headers, date)
            self.remote_repository.update("stocks", ["ticker"], stock_, headers)
        except:
            try:
                self.add_stock_price(stock_, headers, date)
                self.remote_repository.update("stocks", ["ticker"], stock_, headers)
            except:
                pass
            pass

    def att_info_yahoo(self, stock_, headers, date=None):
        try:
            prices = self.http_repository.get(
                self.get_url_bff() + 'yahoofinance/info-br/' + stock_['ticker'],
                headers=headers
            ).json()

            try:
                stock_['price'] = prices['price']['regularMarketPrice']
            except Exception as e:
                self.logger.exception(e)
                pass
            try:
                stock_['ebitda'] = prices['financial_data']['ebitda']
            except Exception as e:
                self.logger.exception(e)
                stock_['ebitda'] = 0
            try:
                stock_['ev'] = prices['key_stats']['enterpriseValue']
            except Exception as e:
                self.logger.exception(e)
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
            self.remote_repository.update("stocks", ["ticker"], stock_, headers)
        except Exception as e:
            self.logger.exception(e)
            try:
                self.add_stock_price(stock_, headers, date)
                self.remote_repository.update("stocks", ["ticker"], stock_, headers)
            except Exception as e:
                self.logger.exception(e)
                pass
            pass

    def att_dividend_info(self, stock, headers):
        select = f"select value_per_quote as value " \
                 f"from dividends_map m, " \
                 f"stocks st " \
                 f"where st.id = m.investment_id " \
                 f"and st.ticker = '{stock['ticker']}' " \
                 f"and date_with >= now() + interval '-1 year' order by date_with desc"
        values = self.remote_repository.execute_select(select, headers)
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
            self.remote_repository.update("stocks", ["ticker"], stock, headers)
        else:
            stock["dy"] = 0
            stock["last_dividend"] = 0
            stock["desv_dy"] = 10000000
            self.remote_repository.update("stocks", ["ticker"], stock, headers)
        return stock

    def get_stocks_consolidated(self, args=None, headers=None):
        stocks = self.get_stocks(args, headers)
        if stocks.__len__() > 0:
            stocks_consolidated = []
            segments = []
            count = 0
            for stock in stocks:
                count += 1
                msg__ = "Atualizando " + str(count) + " de " + str(stocks.__len__()) + " - " + str(
                    stock['investment_id'])
                self.logger.info(msg__)
                if stock['quantity'] > 0:
                    segment = {}
                    stock_consolidated = {}
                    investment_type = self.remote_repository.get_object("stocks", ["id"],
                                                                        {"id": stock['investment_id']},
                                                                        headers)
                    ticker_ = investment_type['ticker']
                    stock['ticker'] = ticker_
                    stock_ = self.remote_repository.get_object("stocks", ["ticker"], stock, headers)
                    stock_, investment_type, att_price_ = self.http_repository.get_values_by_ticker(stock_,
                                                                                                    False,
                                                                                                    headers,
                                                                                                    2)
                    if att_price_:
                        if investment_type['id'] == 100:
                            self.att_price_yahoo_us(stock_, headers)
                        # else:
                        #     if investment_type['id'] != 16:
                        #         self.att_price_yahoo(stock_, headers)

                    if investment_type['id'] == 16:
                        stock_price = self.fixed_income_handler.get_stock_price_by_ticker(
                            ticker_,
                            headers,
                            (datetime.now()).strftime('%Y-%m-%d')
                        )
                        stock_['price'] = stock_price['price']
                    stock_ = self.att_dividend_info(stock_, headers)
                    data_ant = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d 23:59:59')
                    price_ant = self.stock_handler.get_price(stock_['id'], data_ant, headers)
                    data_atu = datetime.now().strftime('%Y-%m-%d 23:59:59')
                    price_atu = self.stock_handler.get_price(stock_['id'], data_atu, headers)

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
                        perc_gain = float(stock_consolidated['price_atu']) / float(stock_consolidated['avg_price']) - 1

                        stock_consolidated['total_value_atu'] = float(stock_consolidated['total_value_invest']) + \
                                                                float(
                                                                    perc_gain * float(
                                                                        stock_consolidated['total_value_invest']))

                        if price_ant is not None:
                            perc_gain_ant = (float(price_ant['price']) / float(stock_consolidated['avg_price'])) - 1

                            stock_consolidated['total_value_ant'] = float(stock_consolidated['total_value_invest']) + \
                                                                    float(
                                                                        perc_gain_ant * float(stock_consolidated[
                                                                                                  'total_value_invest']))

                            stock_consolidated['value_ant_date'] = datetime \
                                .strptime(price_ant['date_value'], '%Y-%m-%d %H:%M:%S.%f') \
                                .replace(tzinfo=timezone.utc).strftime('%Y-%m-%d')
                            stock_consolidated['variation'] = stock_consolidated['total_value_atu'] - \
                                                              stock_consolidated['total_value_ant']
                            stock_consolidated['variation_perc'] = stock_consolidated['total_value_atu'] / \
                                                                   stock_consolidated['total_value_ant'] - 1
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
                            stock_consolidated['variation_perc'] = stock_consolidated['total_value_atu'] / \
                                                                   stock_consolidated['total_value_ant'] - 1
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
        stocks_br = self.stock_handler.get_stocks(1, headers, args)
        bdrs = self.stock_handler.get_stocks(4, headers, args)
        fiis = self.fii_handler.get_fiis(headers, args)
        founds = self.stock_handler.get_founds(15, headers)
        fix_income = self.stock_handler.get_founds(16, headers)
        criptos = self.stock_handler.get_founds(100, headers)
        return stocks_br, bdrs, fiis, founds, fix_income, criptos

    def buy_sell_indication(self, args=None, headers=None):
        infos = self.get_stocks_consolidated(args, headers)
        infos['stocks_names'] = self.stock_handler.get_stocks_basic(headers)

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
            _ = self.remote_repository.get_all_objects("user_recomendations", headers)
            if len(_) > 0:
                self.remote_repository.delete_all("user_recomendations", headers)
            self.save_recomendations(info['stocks_br'], headers)
            self.save_recomendations(info['bdrs'], headers)
            self.save_recomendations(info['fiis'], headers)
            self.save_recomendations(info['founds'], headers)
            self.save_recomendations(info['fix_income'], headers)
            self.save_recomendations(info['criptos'], headers)
        except Exception as e:
            self.logger.exception(e)
            pass

    def save_recomendations(self, recomendations, headers=None):
        for recomendation in recomendations:
            recomendation["investment_id"] = \
                self.remote_repository.get_object("stocks", ["ticker"], recomendation, headers)['id']
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
                self.remote_repository.insert("user_recomendations", recomendation, headers)
            except Exception as e:
                self.logger.exception(e)
                pass

    def save_type_gruped(self, infos, headers=None):
        type_grouped = infos['type_grouped']
        for type_ in type_grouped:
            type_['id'] = type_['type_id']
            self.remote_repository.update("type_gruped_values", ["id"], type_, headers)

    def save_resume(self, infos, headers=None):
        resume = infos['resume']
        resume['id'] = 9999
        resume['type_id'] = 9999
        self.remote_repository.update("resume_values", ["id"], resume, headers)

    def save_infos(self, infos, headers=None):
        resume = None
        try:
            resume = self.remote_repository.get_object("investments_calc_resume", [], {}, headers)
        except:
            pass
        j_infos = json.dumps(infos, cls=Encoder, ensure_ascii=False)
        if resume is None:
            resume = {'resume': j_infos}
            self.remote_repository.insert("investments_calc_resume", resume, headers)
        else:
            resume['resume'] = j_infos
            self.remote_repository.update("investments_calc_resume", ["id"], resume, headers)

    def add_dividend_info(self, stock, headers=None):
        stock_ = self.remote_repository.get_object("stocks", ["ticker"], stock, headers)
        arg = {
            'investment_id': stock_['id'],
            'active': 'S'
        }
        dividends = self.dividend_handler.get_dividends(arg, headers)
        stock['dividends'] = 0
        for dividend in dividends:
            stock['dividends'] += float(dividend['quantity'] * dividend['value_per_quote'])
        stock['dyr'] = stock['dividends'] / stock['total_value_atu']
        return stock

    def add_total_dividends_info(self, infos, headers=None):
        arg = {}
        dividends = self.dividend_handler.get_dividends(arg, headers)
        infos['resume']['total_dividends'] = 0
        for dividend in dividends:
            infos['resume']['total_dividends'] += float(dividend['quantity'] * dividend['value_per_quote'])
        infos['resume']['dyr'] = infos['resume']['total_dividends'] / infos['resume']['total_value_atu']
        return infos

    def add_total_profit_loss_info(self, infos, headers=None, args=None):
        profit_loss = self.remote_repository.get_objects("profit_loss", data=args, headers=headers)
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
            'user_id': self.remote_repository.get_user(headers)['id'],
            'movement_type': 1
        }
        movements = self.remote_repository.get_objects("user_stocks_movements",
                                                       data=filter_,
                                                       headers=headers)
        days = 0
        quantity = 0
        for movement in movements:
            date_ = datetime.strptime(movement['date'], '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)
            now_ = datetime.now().astimezone(tz=timezone.utc)
            delta = now_ - date_

            days += delta.days * movement['quantity'] * movement['price']
            quantity += movement['quantity'] * movement['price']

        avg_days = days / quantity
        if avg_days is None or avg_days < 1:
            avg_days = 1
        infos['resume']['gain'] = infos['resume']['total_value_atu'] / infos['resume']['total_value_invest'] - 1
        infos['resume']['daily_gain'] = self.root_n(infos['resume']['gain'] + 1, avg_days) - 1
        infos['resume']['daily_dyr'] = self.root_n(infos['resume']['dyr'] + 1, avg_days) - 1
        profits_r = (infos['resume']['profits'] - infos['resume']['losses']) / infos['resume']['total_value_atu']
        infos['resume']['daily_flr'] = self.root_n(profits_r + 1, avg_days) - 1

        total_gain_real = ((infos['resume']['total_value_atu'] - infos['resume']['total_value_invest']) +
                           (infos['resume']['profits'] - infos['resume']['losses']) +
                           infos['resume']['total_dividends'])
        real_gain = total_gain_real / infos['resume']['total_value_atu']
        real_gain = self.root_n(real_gain + 1, avg_days) - 1

        infos['resume']['daily_total_gain'] = real_gain
        infos['resume']['monthly_gain'] = (infos['resume']['daily_total_gain'] + float(1)) ** float(30) - float(1)

        infos['resume']['total_gain'] = infos['resume']['dyr'] + infos['resume']['gain']
        infos['resume']['avg_days'] = avg_days
        return infos

    def root_n(self, x, n):
        return x ** (1 / float(n))

    def add_daily_gain(self, stock, headers=None):
        stock_ = self.remote_repository.get_object("stocks", ["ticker"], stock, headers)
        filter_ = {
            'investment_id': stock_['id'],
            'user_id': self.remote_repository.get_user(headers)['id'],
            'movement_type': 1
        }
        buy_movements = self.remote_repository.get_objects("user_stocks_movements",
                                                           data=filter_, headers=headers)
        filter_['movement_type'] = 2
        sell_movements = self.remote_repository.get_objects("user_stocks_movements",
                                                            data=filter_, headers=headers)
        days = 0
        buy_movements = sorted(buy_movements, key=lambda x: x['date'])
        for movement in buy_movements:
            date_ = datetime.strptime(movement['date'], '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)
            now_ = datetime.now().astimezone(tz=timezone.utc)
            quantity_ = movement['quantity']
            for sell_movement in sell_movements:
                quantity_sell_ = sell_movement['quantity']
                if quantity_ > quantity_sell_ > 0 and quantity_ > 0:
                    sell_movement['quantity'] = 0
                    quantity_ = quantity_ - quantity_sell_
                elif quantity_ > 0 and quantity_sell_ > 0:
                    quantity_sell_ = quantity_sell_ - quantity_
                    sell_movement['quantity'] = quantity_sell_
                    quantity_ = 0

            delta = now_ - date_
            days += delta.days * quantity_

        avg_days = days / stock['quantity']
        if avg_days is None or avg_days < 1:
            avg_days = 1
        filter_ = {
            'investment_id': stock_['id'],
        }
        user_stock_ = self.remote_repository.get_object("user_stocks", data=filter_, headers=headers)
        if user_stock_['avg_days'] != avg_days:
            user_stock_['avg_days'] = avg_days
            self.remote_repository.insert("user_stocks", user_stock_, headers)

        stock['daily_gain'] = self.root_n(stock['gain'] + 1, avg_days) - 1
        stock['daily_dyr'] = self.root_n(stock['dyr'] + 1, avg_days) - 1
        stock['avg_days'] = avg_days
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
        stock_ = self.remote_repository.get_object("stocks", ["ticker"], stock, headers)
        filter_ = {
            'investment_id': stock_['id'],
        }
        stock_notification_config = self.remote_repository.get_object(
            "stock_notification_config",
            ["investment_id"],
            filter_,
            headers
        )
        if stock_notification_config is None:
            return None
        else:
            if not (Utils.work_day() and Utils.work_time()):
                return stock_notification_config
            if stock_notification_config['monthly_gain_target'] is not None and \
                    stock_notification_config['monthly_gain_target'] / 100 < stock['monthly_gain'] and \
                    self.check_time_to_notfy(stock_notification_config, headers):
                message = 'A ação ' + stock['ticker'] + ' atingiu o ganho mensal esperado de ' + \
                          str(stock_notification_config['monthly_gain_target']) + '%. Ganho atual: ' + \
                          str(stock['monthly_gain'] * 100) + '%'
                Utils.inform_to_client(
                    stock,
                    "buySellRecommendation",
                    headers,
                    message,
                    "Buy Sell Recommendation"
                )
                stock_notification_config['last_notification'] = datetime \
                    .strftime(datetime.now(pytz.utc), '%Y-%m-%d %H:%M:%S.%f')
                self.remote_repository.update(
                    "stock_notification_config",
                    ["id"],
                    stock_notification_config,
                    headers
                )
            elif stock_notification_config['value_target'] is not None and \
                    stock_notification_config['value_target'] < stock['price_atu'] and \
                    self.check_time_to_notfy(stock_notification_config, headers):
                message = 'A ação ' + stock['ticker'] + ' atingiu o valor desejado de ' + \
                          str(stock_notification_config['value_target']) + '. Valor atual: ' + \
                          str(stock['price_atu'])
                Utils.inform_to_client(
                    stock,
                    "buySellRecommendation",
                    headers,
                    message,
                    "Buy Sell Recommendation"
                )
                stock_notification_config['last_notification'] = datetime \
                    .strftime(datetime.now(pytz.utc), '%Y-%m-%d %H:%M:%S.%f')
                self.remote_repository.update(
                    "stock_notification_config",
                    ["id"],
                    stock_notification_config,
                    headers
                )
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
            self.logger.exception(e)
            return True

    def set_buy_sell_info(self, stock_, stock_ref, types_sum, stocks, headers=None, notify=False):
        min_days_to_sell = 180
        great_gain = 0.07
        max_rank_to_buy = 20
        great_gain_ = great_gain

        perc_refer = 'ticker_weight_in_all'
        stock_['buy_sell_indicator'] = "neutral"
        stock_ = self.set_stock_weight(stock_, types_sum, stocks)
        stock_['ticker_weight_ideal'] = self.get_ticker_weight_ideal(stock_['ticker'], headers)
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
            remove_avg_days = False
            if 'avg_days' not in stock_:
                remove_avg_days = True
                stock_['avg_days'] = 0
            if rank <= max_rank_to_buy:
                if (stock_['monthly_gain'] > great_gain
                        and not self.is_not_in_ideal_perc(stock_, types_sum, True)
                        and stock_['avg_days'] > min_days_to_sell):
                    stock_['buy_sell_indicator'] = "great-gain"
                    stock_['recommendation'] = (
                            "Favorável à venda - ganho acima do esperado (carência expirada)-> " +
                            self.to_percent_from_aliq(stock_['monthly_gain']) +
                            " -> Vender " + self.to_brl(self.get_tot_to_sell(types_sum[0],
                                                                             stock_['ticker_weight_ideal'],
                                                                             stock_['ticker_weight_in_all'])) +
                            " -> Rank: " + str(rank)
                    )
                elif self.is_not_in_ideal_perc(stock_, types_sum, True):
                    stock_['buy_sell_indicator'] = "buy"
                    stock_['recommendation'] = (
                            "Favorável à compra " +
                            " -> Comprar " + self.to_brl(self.get_tot_to_buy(types_sum[0],
                                                                             stock_['ticker_weight_ideal'],
                                                                             stock_['ticker_weight_in_all']))
                            + " -> Rank: " + str(rank))
                else:
                    stock_['recommendation'] = "Manter posição atual -> Rank: " + str(rank)
            elif rank > 40:
                if stock_['monthly_gain'] > great_gain:
                    stock_['buy_sell_indicator'] = "great-gain"
                    stock_['recommendation'] = (
                            "Favorável à venda - ganho acima do esperado -> " +
                            self.to_percent_from_aliq(stock_['monthly_gain']) +
                            " -> Vender " + self.to_brl(types_sum[0] * stock_['ticker_weight_in_all']) +
                            " -> Rank: " + str(rank)
                    )
                elif self.is_not_in_ideal_perc(stock_, types_sum):
                    stock_['buy_sell_indicator'] = "sell"
                    stock_['recommendation'] = (
                            "Favorável à venda - manter somente o ideal -> " +
                            self.to_percent_from_aliq(stock_['monthly_gain']) +
                            " -> Vender " + self.to_brl(self.get_tot_to_sell(types_sum[0],
                                                                             stock_['ticker_weight_ideal'],
                                                                             stock_['ticker_weight_in_all'])) +
                            " -> Rank: " + str(rank)
                    )
                elif stock_['avg_days'] > (min_days_to_sell * 3):
                    stock_['buy_sell_indicator'] = "sell"
                    stock_['recommendation'] = (
                            "Favorável à venda - carência de venda 3x expirada -> " +
                            self.to_percent_from_aliq(stock_['monthly_gain']) +
                            " -> Vender " + self.to_brl(types_sum[0] * stock_['ticker_weight_in_all']) +
                            " -> Rank: " + str(rank)
                    )
                elif stock_['avg_days'] > 0:
                    stock_['recommendation'] = "Manter posição atual (Período de carência x3) -> Rank: " + str(rank)
                else:
                    stock_['buy_sell_indicator'] = "sell"
                    stock_['recommendation'] = "Não comprar -> Rank: " + str(rank)
            else:
                if (stock_['monthly_gain'] > great_gain
                        and stock_['avg_days'] > min_days_to_sell):
                    stock_['buy_sell_indicator'] = "great-gain"
                    stock_['recommendation'] = (
                            "Favorável à venda - ganho acima do esperado (carência expirada)-> " +
                            self.to_percent_from_aliq(stock_['monthly_gain']) +
                            " -> Vender " + self.to_brl(types_sum[0] * stock_['ticker_weight_in_all']) +
                            " -> Rank: " + str(rank)
                    )
                elif (self.is_not_in_ideal_perc(stock_, types_sum)
                      and stock_['avg_days'] > (min_days_to_sell * 2)):
                    stock_['buy_sell_indicator'] = "sell"
                    stock_['recommendation'] = (
                            "Favorável à venda - manter somente o ideal (carência x2 expirada)-> " +
                            self.to_percent_from_aliq(stock_['monthly_gain']) +
                            " -> Vender " + self.to_brl(self.get_tot_to_sell(types_sum[0],
                                                                             stock_['ticker_weight_ideal'],
                                                                             stock_['ticker_weight_in_all'])) +
                            " -> Rank: " + str(rank)
                    )
                else:
                    stock_['recommendation'] = "Manter posição atual -> Rank: " + str(rank)

            if remove_avg_days:
                del stock_['avg_days']

        else:
            if stock_['monthly_gain'] > great_gain:
                stock_['buy_sell_indicator'] = "great-gain"
                stock_['recommendation'] = (
                        "Favorável à venda - ganho acima do esperado -> " +
                        self.to_percent_from_aliq(stock_['monthly_gain']) +
                        " -> Vender " + self.to_brl(types_sum[0] * stock_['ticker_weight_in_all']) +
                        " -> Não ranqueada nas recomendações"
                )
            elif self.is_not_in_ideal_perc(stock_, types_sum):
                stock_['buy_sell_indicator'] = "sell"
                stock_['recommendation'] = (
                        "Favorável à venda - manter somente o ideal -> " +
                        self.to_percent_from_aliq(stock_['monthly_gain']) +
                        " -> Vender " + self.to_brl(self.get_tot_to_sell(types_sum[0],
                                                                         stock_['ticker_weight_ideal'],
                                                                         stock_['ticker_weight_in_all'])) +
                        " -> Não ranqueada nas recomendações"
                )
            elif stock_['avg_days'] > min_days_to_sell:
                stock_['buy_sell_indicator'] = "sell"
                stock_['recommendation'] = (
                        "Favorável à venda - carência de venda expirada -> " +
                        self.to_percent_from_aliq(stock_['monthly_gain']) +
                        " -> Vender " + self.to_brl(types_sum[0] * stock_['ticker_weight_in_all']) +
                        " -> Não ranqueada nas recomendações"
                )
            elif stock_['avg_days'] > 0:
                stock_['recommendation'] = ("Manter posição atual (Período de carência) "
                                            "-> Não ranqueada nas recomendações")
            else:
                stock_['buy_sell_indicator'] = "sell"
                stock_['recommendation'] = "Não comprar -> Não ranqueada nas recomendações"

    def is_not_in_ideal_perc(self, stock_, types_sum=None, buy=False):
        if buy:
            is_not_ideal_ = stock_['ticker_weight_in_all'] < stock_['ticker_weight_ideal']
        else:
            is_not_ideal_ = stock_['ticker_weight_in_all'] > stock_['ticker_weight_ideal']
        if is_not_ideal_ and types_sum is not None:
            if buy:
                value = self.get_tot_to_buy(types_sum[0],
                                            stock_['ticker_weight_ideal'],
                                            stock_['ticker_weight_in_all'])
            else:
                value = self.get_tot_to_sell(types_sum[0],
                                             stock_['ticker_weight_ideal'],
                                             stock_['ticker_weight_in_all'])
            try:
                price = stock_['price_atu']
            except:
                try:
                    price = stock_['price']
                except:
                    return is_not_ideal_
            return value > price
        return is_not_ideal_

    def get_ticker_weight_ideal(self, ticker, headers=None, rank=20):
        try:
            select = f"select  us.ticker as ticker, \
                               piit.perc_ideal as perc_ideal_type, \
                               uic.coef as coef, \
                               uic.coef / (select sum(uic_2.coef) \
                                           from user_invest_configs uic_2 \
                                           where uic_2.user_id = uic.user_id \
                                             and uic_2.investment_id in (select investment_id  \
                                                                         from user_stocks us_2  \
                                                                         where us_2.user_id = uic.user_id  \
                                                                           and us_2.quantity > 0  \
                                                                           and us_2.investment_type_id = us.investment_type_id  \
                                                                         union  \
                                                                         select investment_id  \
                                                                         from user_recomendations ur  \
                                                                         where ur.user_id = uic.user_id  \
                                                                           and ur.investment_type_id = us.investment_type_id  \
                                                                           and rank <= {rank})) * piit.perc_ideal as perc_ideal \
                        from user_invest_configs uic,  \
                             stocks us,  \
                             perc_ideal_investment_type piit  \
                        where uic.user_id = piit.user_id  \
                          and uic.investment_id = us.id  \
                          and us.investment_type_id = piit.investment_type_id  \
                          and uic.user_id = :user_id  \
                          and us.ticker = '{ticker}'"
            info = self.remote_repository.execute_select(select, headers)
            if len(info) > 0:
                return info[0]['perc_ideal'] / 100
            else:
                return 0
        except Exception as e:
            self.logger.exception(e)
            return 0

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

    def get_tot_to_buy(self, total, perc_ideal, perc_atu):
        v_atu = total * perc_atu
        # new_tot = total + (total * (perc_ideal / 100) - v_atu)
        new_val = total * perc_ideal
        buy = new_val - v_atu
        return buy

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
        stocks = self.remote_repository.get_objects("stocks", [], {}, headers)
        stocks = self.calc_ranks(stocks)
        self.remote_repository.insert("stocks", stocks, headers)

    def att_stock_price_new(self, headers, daily, stock, stock_, type, price_type="4", reimport=False, data_=None):
        if stock_['prices_imported'] == 'N' or daily or reimport:
            if type == 'fundo' and not daily or type == 'fundo' and reimport:
                company_ = stock_['url_infos']
                company_ = company_.replace('/fundos-de-investimento/', '')
                infos = self.http_repository.get_prices_fundos(company_, price_type == "1")
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
                infos = self.http_repository.get_prices(stock_['ticker'], type, daily, price_type)
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
            self.remote_repository.update("stocks", ["ticker"], stock_, headers)
        self.logger.info(f"{stock_['ticker']} - {stock_['name']} - atualizado")

    def investment_facts(self, facts, headers=None):
        user_invest_configs = {}
        for fact in facts:
            fact_ = None
            try:
                fact['id']
                fact_ = self.remote_repository.get_object("user_invest_facts", ["id"], fact, headers)
            except KeyError:
                fact['id'] = None

            filter = {
                "ticker": fact['ticker'],
            }
            stock_ = self.remote_repository.get_object("stocks", ["ticker"], filter, headers)
            fact['investment_id'] = stock_['id']
            del fact['ticker']
            if fact['id'] is None:
                self.remote_repository.insert("user_invest_facts", fact, headers)
            else:
                self.remote_repository.update("user_invest_facts", ["id"], fact, headers)
            filter = {
                "investment_id": fact['investment_id'],
            }
            user_invest_configs = self.remote_repository.get_object("user_invest_configs", ["investment_id"],
                                                                    filter, headers)
            if user_invest_configs is None:
                user_invest_configs = {
                    "investment_id": fact['investment_id'],
                    "coef": 1
                }
                self.remote_repository.insert("user_invest_configs", user_invest_configs, headers)

            weight = fact['weight']
            if weight is None:
                weight = 10
            if fact_ is not None:
                weight = fact_['weight']
                if fact['yes_no'] != fact_['yes_no']:
                    weight = weight + fact['weight']
                else:
                    weight = fact['weight'] - fact_['weight']
            if fact['yes_no'] == 'no':
                weight = weight * -1
            fact['weight'] = weight
            user_invest_configs['coef'] = user_invest_configs['coef'] + (weight / 100)
            self.remote_repository.update("user_invest_configs", ["investment_id"], user_invest_configs, headers)
        return user_invest_configs

    def get_investment_facts_labels(self, headers=None):
        select = Utils.read_file("static/FactsLabels.sql")
        return self.remote_repository.execute_select(select, headers)

    def get_investment_facts(self, ticker, headers=None):
        filter = {
            "ticker": ticker,
        }
        stock_ = self.remote_repository.get_object("stocks", ["ticker"], filter, headers)
        filter = {
            "investment_id": stock_['id'],
        }
        facts = self.remote_repository.get_objects("user_invest_facts", ["investment_id"], filter, headers)
        for fact in facts:
            del fact['investment_id']
            fact['ticker'] = ticker
        return facts

    def investment_calc(self, data, headers=None):
        user_invest_apply = {'amount': data['amount']}
        self.remote_repository.insert("user_invest_apply", user_invest_apply, headers)
        select = f"select * from user_invest_apply where user_id = :user_id " \
                 f" order by apply_date desc limit 1"
        user_invest_apply = self.remote_repository.execute_select(select, headers)[0]
        perc_ideal_investment_types = self.remote_repository.get_all_objects("perc_ideal_investment_type", headers)
        amount_types = []
        resume = self.remote_repository.get_object("resume_values", [], {}, headers)
        total_amount = 0
        resto_total = 0
        config_ = self.remote_repository.get_object("configs", data={}, headers=headers)
        for investment_type in perc_ideal_investment_types:
            perc_ideal = investment_type['perc_ideal'] / 100
            type_gruped_values = self.remote_repository.get_object("type_gruped_values", ["type_id"],
                                                                   {"type_id": investment_type['investment_type_id']},
                                                                   headers)

            amount_atu = type_gruped_values['total_value_atu']
            amount = perc_ideal * (resume['total_value_atu'] + user_invest_apply['amount'])
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
                self.remote_repository.insert("user_invest_apply_type", amount_type, headers)

            investment_type_id = amount_type['investment_type_id']
            total_amount_stock = 0
            amount_stocks = []
            user_recomendations = []
            if investment_type_id == 16:
                amount = amount_type['amount']
                stock_ = self.remote_repository.get_object("stocks", data={"ticker": "Renda fixa"}, headers=headers)
                total_amount_stock = total_amount_stock + amount
                amount_stocks.append({'investment_id': stock_['id'], 'amount': amount,
                                      'user_invest_apply_id': user_invest_apply['id']})

            else:
                select = f"select * from user_recomendations \
                            where user_id = :user_id \
                                    and investment_type_id = {amount_type['investment_type_id']} \
                                    and (rank <= {config_['rank']} \
                                            or investment_id in (select us.investment_id from user_stocks us \
                                                                where us.user_id = :user_id  \
                                                                    and us.investment_type_id = \
                                                                    {amount_type['investment_type_id']} \
                                                                    and us.quantity >0)) \
                         order by rank"
                user_recomendations = self.remote_repository.execute_select(select, headers)
                for user_recomendation in user_recomendations:
                    amount, stock_ = self.get_amount_value(user_recomendation,
                                                           resume,
                                                           headers,
                                                           config_['rank'],
                                                           user_invest_apply['amount'])

                    valid = self.get_is_valid_buy(stock_, headers)
                    if (amount > stock_['price'] or stock_['investment_type_id'] == 100) and valid:
                        total_amount_stock = total_amount_stock + amount
                        amount_stocks.append({'investment_id': user_recomendation['investment_id'], 'amount': amount,
                                              'user_invest_apply_id': user_invest_apply['id']})
            amount_stocks_ajust = []
            amount_ajsut = 0
            for amount_stock in amount_stocks:
                stock_ = \
                    self.remote_repository.get_object("stocks", ["id"], {"id": amount_stock['investment_id']}, headers)
                amount = amount_stock['amount']
                if amount > stock_['price'] or stock_['investment_type_id'] == 100:
                    amount_stock['amount'] = amount_type['amount'] * (amount / total_amount_stock)
                    if amount_stock['amount'] > amount:
                        amount_stock['amount'] = amount
                    if amount_stock['amount'] > stock_['price'] or stock_['investment_type_id'] == 100:
                        if stock_['investment_type_id'] == 100:
                            num_quotas = amount_stock['amount'] / stock_['price']
                        else:
                            num_quotas = int(amount_stock['amount'] / stock_['price'])
                        amount_stock['amount'] = num_quotas * stock_['price']
                        amount_stock['num_quotas'] = num_quotas
                        amount_ajsut = amount_ajsut + amount_stock['amount']
                        amount_stocks_ajust.append(amount_stock)
            amount_stock_temp = 0
            if amount_ajsut > 0:
                for ajsut in amount_stocks_ajust:
                    amount_ = ajsut['amount']
                    stock_ = \
                        self.remote_repository.get_object("stocks", ["id"], {"id": ajsut['investment_id']}, headers)
                    ajsut['amount'] = amount_type['amount'] * (ajsut['amount'] / amount_ajsut)
                    if ajsut['amount'] > amount_:
                        ajsut['amount'] = amount_
                    if stock_['investment_type_id'] == 100:
                        num_quotas = ajsut['amount'] / stock_['price']
                    else:
                        num_quotas = int(ajsut['amount'] / stock_['price'])
                    ajsut['amount'] = num_quotas * stock_['price']
                    ajsut['num_quotas'] = num_quotas
                    amount_stock_temp = amount_stock_temp + ajsut['amount']
                    self.remote_repository.insert("user_invest_apply_stock", ajsut, headers)
            resto = amount_type['amount'] - amount_stock_temp
            continuar = True
            while continuar and resto > 0:
                if user_recomendations.__len__() == 0:
                    resto_temp = resto
                for user_recomendation in user_recomendations:
                    resto_temp = resto
                    amount, stock_ = self.get_amount_value(user_recomendation,
                                                           resume,
                                                           headers,
                                                           config_['rank'],
                                                           user_invest_apply['amount'])
                    valid = self.get_is_valid_buy(stock_, headers)
                    if resto > stock_['price'] and amount > 0 and valid:
                        user_invest_apply_stock = \
                            self.remote_repository.get_object("user_invest_apply_stock",
                                                              ["investment_id", "user_invest_apply_id"],
                                                              {"investment_id": user_recomendation['investment_id'],
                                                               "user_invest_apply_id": user_invest_apply['id']},
                                                              headers)
                        if user_invest_apply_stock is None:
                            amount_stock = {'investment_id': user_recomendation['investment_id'],
                                            'amount': 0,
                                            'num_quotas': 0,
                                            'user_invest_apply_id': user_invest_apply['id']}
                            self.remote_repository.insert("user_invest_apply_stock", amount_stock, headers)
                            user_invest_apply_stock = \
                                self.remote_repository.get_object("user_invest_apply_stock",
                                                                  ["investment_id", "user_invest_apply_id"],
                                                                  {"investment_id": user_recomendation['investment_id'],
                                                                   "user_invest_apply_id": user_invest_apply['id']},
                                                                  headers)
                        if user_invest_apply_stock['amount'] < amount:
                            user_invest_apply_stock['amount'] = user_invest_apply_stock['amount'] + stock_['price']
                            user_invest_apply_stock['num_quotas'] = user_invest_apply_stock['num_quotas'] + 1
                            self.remote_repository.update("user_invest_apply_stock", ["id"], user_invest_apply_stock,
                                                          headers)
                            resto = resto - stock_['price']
                if resto_temp == resto:
                    continuar = False
                    self.logger.info(resto)
                    self.logger.info(amount_type['investment_type_id'])
                    resto_total = resto_total + resto
                    break
        if resto_total > 0:
            self.logger.info(resto_total)
            stock_ = self.remote_repository.get_object(
                "stocks",
                data={"ticker": "Renda fixa"},
                headers=headers
            )
            user_invest_apply_stock = self.remote_repository.get_object(
                "user_invest_apply_stock",
                ["investment_id", "user_invest_apply_id"],
                {
                    "investment_id": stock_['id'],
                    "user_invest_apply_id": user_invest_apply['id']
                },
                headers
            )

            if user_invest_apply_stock is None:
                amount_stock = {'investment_id': stock_['id'],
                                'amount': resto_total,
                                'num_quotas': resto_total,
                                'user_invest_apply_id': user_invest_apply['id']}
                self.remote_repository.insert("user_invest_apply_stock", amount_stock, headers)
            else:
                user_invest_apply_stock['amount'] = user_invest_apply_stock['amount'] + resto_total
                user_invest_apply_stock['num_quotas'] = user_invest_apply_stock['num_quotas'] + resto_total
                self.remote_repository.update("user_invest_apply_stock", ["id"], user_invest_apply_stock,
                                              headers)

        self.get_sell_sugestions(user_invest_apply, headers)
        return self.remote_repository.get_objects(
            "user_invest_apply_stock",
            ['user_invest_apply_id'],
            {'user_invest_apply_id': user_invest_apply['id']},
            headers
        )

    def get_is_valid_buy(self, stock_, headers):
        tiker_prefix = ''.join([i for i in stock_['ticker'] if not i.isdigit()])
        select = (f"select * from user_stocks us, "
                  f"stocks s "
                  f"where s.id = us.investment_id  and quantity > 0 "
                  f"and user_id = :user_id "
                  f"and s.ticker <> '{stock_['ticker']}' "
                  f"and s.ticker like '{tiker_prefix}%'")
        user_stocks_ = self.remote_repository.execute_select(select, headers)
        valid = True
        if user_stocks_.__len__() > 0:
            valid = False
        return valid

    def get_amount_value(self, user_recomendation, resume, headers, rank=20, aport_value=0):
        stock_ = self.remote_repository.get_object(
            "stocks",
            ["id"],
            {"id": user_recomendation['investment_id']},
            headers
        )
        perc_ideal = self.get_ticker_weight_ideal(stock_['ticker'], headers, rank)

        user_stock = self.remote_repository.get_object(
            "user_stocks",
            ["investment_id"],
            user_recomendation,
            headers
        )
        if stock_['segment_custom'] is not None:
            segment_ = stock_['segment_custom']
        else:
            segment_ = stock_["segment"]
        user_segments_configs = self.remote_repository.get_object(
            "user_segments_configs",
            ["segment_name"],
            {"segment_name": segment_},
            headers
        )
        if user_stock is not None:
            amount_atu = user_stock['quantity'] * stock_['price']
        else:
            amount_atu = 0
        amount = perc_ideal * (resume['total_value_atu'] + aport_value)
        amount = amount - amount_atu
        if user_segments_configs is not None:
            perc_atu_segment = user_recomendation['segment_weight_in_all']
            value_atu_segment = perc_atu_segment * resume['total_value_atu'] + amount
            new_ideal_segment_value = user_segments_configs['ideal_prec_max'] * (
                    resume['total_value_atu'] + aport_value)
            if value_atu_segment > new_ideal_segment_value:
                diff = new_ideal_segment_value - value_atu_segment
                amount = amount - diff
                if amount < 0:
                    amount = 0
        return amount, stock_

    def get_sell_sugestions(self, user_invest_apply, headers=None):

        user_stocks = self.remote_repository.get_all_objects("user_stocks", headers)
        user_stocks = [user_stock for user_stock in user_stocks if user_stock['quantity'] > 0]
        for user_stock in user_stocks:
            stock_ = \
                self.remote_repository.get_object("stocks", ["id"], {"id": user_stock['investment_id']}, headers)
            user_recommendation = \
                self.remote_repository.get_object("user_recomendations", ["investment_id"], user_stock, headers)

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
                        self.remote_repository.insert("user_invest_sell_stock", user_invest_sell_stock, headers)
            else:
                user_invest_sell_stock["motive"] = "not recommended"
                self.remote_repository.insert("user_invest_sell_stock", user_invest_sell_stock, headers)

    def last_investment_calc(self, headers=None):
        user_ = self.remote_repository.get_object("users", data={}, headers=headers)
        select = f"select * from user_invest_apply where user_id = {user_['id']}" \
                 f" order by apply_date desc limit 1"
        user_invest_apply = self.remote_repository.execute_select(select, headers)[0]
        return self.investment_calc_info(user_invest_apply, headers)

    def investment_calc_info(self, user_invest_apply, headers):
        stock_recomendations = self.remote_repository.get_objects(
            "user_invest_apply_stock",
            ['user_invest_apply_id'],
            {'user_invest_apply_id': user_invest_apply['id']},
            headers
        )
        stock_recomendations_ = []
        for stock_recomendation in stock_recomendations:
            stock_ = self.remote_repository.get_object(
                "stocks",
                ["id"],
                {"id": stock_recomendation['investment_id']},
                headers
            )
            user_recomendation_ = self.remote_repository.get_object(
                "user_recomendations",
                data={"investment_id": stock_['id']},
                headers=headers
            )
            if user_recomendation_ is None:
                user_recomendation_ = {
                    "rank": 1
                }
            user_stock_ = self.remote_repository.get_object(
                "user_stocks",
                data={"investment_id": stock_['id']},
                headers=headers
            )
            if user_stock_ is None:
                user_stock_ = {
                    "quantity": 0
                }

            if user_recomendation_['rank'] > 10000:
                user_recomendation_['rank'] = user_recomendation_['rank'] - 10000

            renda_fixa = "S" if stock_['investment_type_id'] == 16 else "N"
            cripto = "S" if stock_['investment_type_id'] == 100 else "N"
            stock_recomendation_ = {
                "id": stock_recomendation['id'],
                "ticker": stock_['ticker'],
                "num_quotas": stock_recomendation['num_quotas'],
                "num_quotas_invested": stock_recomendation['num_quotas_invested'],
                "value_or_cotas": 0,
                "tax_or_price": 0,
                "avg_value_quota_invested": stock_recomendation['avg_value_quota_invested'],
                "amount": stock_recomendation['amount'],
                "invested": stock_recomendation['invested'],
                "renda_fixa": renda_fixa,
                "cripto": cripto,
                "investment_type_id": stock_['investment_type_id'],
                "order": 0 if stock_recomendation['invested'] == 'N' else 1,
                "rank": user_recomendation_['rank'],
                "quantity": user_stock_['quantity']
            }
            stock_recomendations_.append(stock_recomendation_)

        stock_recomendations_ = sorted(stock_recomendations_,
                                       key=lambda k: (k['order'], k['investment_type_id'], k['rank']))
        type_invest_recomendations = self.remote_repository.get_objects(
            "user_invest_apply_type",
            ['user_invest_apply_id'],
            {'user_invest_apply_id': user_invest_apply['id']},
            headers
        )
        type_invest_recomendations_ = []
        for type_invest_recomendation in type_invest_recomendations:
            type_invest_recomendation_ = {
                "id": type_invest_recomendation['id'],
                "investment_type_id": type_invest_recomendation['investment_type_id'],
                "investment_type": self.remote_repository.get_object(
                    "investment_types", ["id"],
                    {"id": type_invest_recomendation['investment_type_id']},
                    headers
                )['name'],
                "amount": type_invest_recomendation['amount'],
                "value_invested": type_invest_recomendation['value_invested']
            }
            type_invest_recomendations_.append(type_invest_recomendation_)

        user_invest_apply['stock_recomendations'] = stock_recomendations_
        user_invest_apply['type_invest_recomendations'] = type_invest_recomendations_

        return user_invest_apply

    def save_all_aplly_stock(self, all_apply_stock, headers):
        for apply_stock in all_apply_stock:
            self.save_aplly_stock(apply_stock, headers)

    def save_aplly_stock(self, apply_stock, headers):
        if 'cancel' not in apply_stock:
            apply_stock['cancel'] = 'N'
        user_invest_apply_stock = self.remote_repository.get_object(
            "user_invest_apply_stock",
            ["id"],
            apply_stock,
            headers
        )
        if (user_invest_apply_stock is not None and user_invest_apply_stock['invested'] == 'N'
                and apply_stock['tax_or_price'] > 0 and apply_stock['value_or_cotas'] > 0
                and apply_stock['cancel'] == 'N'):
            user_invest_apply = self.remote_repository.get_object(
                "user_invest_apply",
                ["id"],
                {"id": user_invest_apply_stock['user_invest_apply_id']},
                headers
            )
            stock_ = self.remote_repository.get_object(
                "stocks",
                data={"id": user_invest_apply_stock['investment_id']},
                headers=headers
            )
            user_invest_apply_type_ = self.remote_repository.get_object(
                "user_invest_apply_type",
                data={
                    "investment_type_id": stock_['investment_type_id'],
                    "user_invest_apply_id": user_invest_apply_stock['user_invest_apply_id']
                },
                headers=headers
            )

            if stock_['investment_type_id'] == 16:
                movement = {
                    "quantity": apply_stock['value_or_cotas'],
                    "ticker": apply_stock['ticker'],
                    "fixed_icome": "S",
                    "price": apply_stock['tax_or_price'],
                    "movement_type": 1,
                    "tx_type": apply_stock['type'],
                    "buy_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "venc_date": apply_stock['date_venc']
                }

                user_invest_apply['value_invested'] = user_invest_apply['value_invested'] + \
                                                      apply_stock['value_or_cotas']

                user_invest_apply_type_['value_invested'] = user_invest_apply_type_['value_invested'] + \
                                                            apply_stock['value_or_cotas']
                avg_value_quota_invested_ = (user_invest_apply_stock['avg_value_quota_invested'] *
                                             user_invest_apply_stock['num_quotas_invested'])
                avg_value_quota_invested_ += (apply_stock['value_or_cotas'] * apply_stock['tax_or_price'])
                user_invest_apply_stock['num_quotas_invested'] += apply_stock['value_or_cotas']
                user_invest_apply_stock['avg_value_quota_invested'] = avg_value_quota_invested_ / \
                                                                      user_invest_apply_stock['num_quotas_invested']
                if user_invest_apply_stock['num_quotas_invested'] >= user_invest_apply_stock['num_quotas']:
                    user_invest_apply_stock['invested'] = 'S'

            else:
                user_invest_apply['value_invested'] = user_invest_apply['value_invested'] + \
                                                      (apply_stock['value_or_cotas'] *
                                                       apply_stock['tax_or_price'])
                movement = {
                    "movement_type": 1,
                    "ticker": apply_stock['ticker'],
                    "quantity": apply_stock['value_or_cotas'],
                    "price": apply_stock['tax_or_price'],
                    "buy_date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
                }
                avg_value_quota_invested_ = (user_invest_apply_stock['avg_value_quota_invested'] *
                                             user_invest_apply_stock['num_quotas_invested'])
                avg_value_quota_invested_ += (apply_stock['value_or_cotas'] * apply_stock['tax_or_price'])
                user_invest_apply_stock['num_quotas_invested'] += apply_stock['value_or_cotas']
                user_invest_apply_stock['avg_value_quota_invested'] = avg_value_quota_invested_ / \
                                                                      user_invest_apply_stock['num_quotas_invested']

                user_invest_apply_type_['value_invested'] = user_invest_apply_type_['value_invested'] + \
                                                            (apply_stock['value_or_cotas'] *
                                                             apply_stock['tax_or_price'])
                diff_ = user_invest_apply_stock['amount'] - (
                        user_invest_apply_stock['num_quotas_invested'] *
                        user_invest_apply_stock['avg_value_quota_invested'])
                if diff_ < user_invest_apply_stock['avg_value_quota_invested']:
                    user_invest_apply_stock['invested'] = 'S'

                    stock_rf_ = self.remote_repository.get_object(
                        "stocks",
                        data={"ticker": "Renda fixa"},
                        headers=headers
                    )
                    user_invest_apply_stock_rf = self.remote_repository.get_object(
                        "user_invest_apply_stock",
                        data={
                            "investment_id": stock_rf_['id'],
                            "user_invest_apply_id": user_invest_apply['id']
                        },
                        headers=headers
                    )

                    user_invest_apply_stock_rf['amount'] = user_invest_apply_stock_rf['amount'] + diff_
                    user_invest_apply_stock_rf['num_quotas'] = user_invest_apply_stock_rf['num_quotas'] + diff_
                    self.remote_repository.update(
                        "user_invest_apply_stock",
                        ["id"],
                        user_invest_apply_stock_rf,
                        headers
                    )
            apply_stock['invested'] = user_invest_apply_stock['invested']

            self.remote_repository.update(
                "user_invest_apply_stock",
                ["id"],
                user_invest_apply_stock,
                headers
            )
            self.remote_repository.update("user_invest_apply", ["id"], user_invest_apply, headers)
            self.remote_repository.update(
                "user_invest_apply_type",
                ["id"],
                user_invest_apply_type_,
                headers
            )

            added_ = {"status": "Sucesso", "message": "Aporte salvo com sucesso"}
            msg_ = "Será gerado o movimento de aplicação a seguir: " + str(movement)
            Utils.inform_to_client(added_, "Aporte", headers, msg_)
            if apply_stock['value_or_cotas'] > 0 and apply_stock['tax_or_price'] > 0:
                self.add_movement(movement, headers)
            return apply_stock
        elif apply_stock['cancel'] == 'S' and user_invest_apply_stock is not None:
            stock_rf_ = self.remote_repository.get_object(
                "stocks",
                data={"ticker": "Renda fixa"},
                headers=headers
            )
            user_invest_apply = self.remote_repository.get_object(
                "user_invest_apply",
                ["id"],
                {"id": user_invest_apply_stock['user_invest_apply_id']},
                headers
            )
            user_invest_apply_stock_rf = self.remote_repository.get_object(
                "user_invest_apply_stock",
                data={
                    "investment_id": stock_rf_['id'],
                    "user_invest_apply_id": user_invest_apply['id']
                },
                headers=headers
            )
            diff_ = user_invest_apply_stock['amount'] - (
                    user_invest_apply_stock['num_quotas_invested'] *
                    user_invest_apply_stock['avg_value_quota_invested'])
            user_invest_apply_stock_rf['amount'] = user_invest_apply_stock_rf['amount'] + diff_
            user_invest_apply_stock_rf['num_quotas'] = user_invest_apply_stock_rf['num_quotas'] + diff_
            self.remote_repository.update(
                "user_invest_apply_stock",
                ["id"],
                user_invest_apply_stock_rf,
                headers
            )
            user_invest_apply_stock['invested'] = 'C'
            self.remote_repository.update(
                "user_invest_apply_stock",
                ["id"],
                user_invest_apply_stock,
                headers
            )
        else:
            self.logger.info(f"Não foi salvo {apply_stock}")
            added_ = {"status": "Cuidado", "message": "Registro sem informação de investimento"}
            msg_ = "Não foi salvo: " + str(apply_stock)
            Utils.inform_to_client(added_, "Aporte", headers, msg_)

    def save_aplly_stock_sell(self, apply_stock, headers):
        user_invest_sell_stock = self.remote_repository.get_object(
            "user_invest_sell_stock",
            ["id"],
            apply_stock,
            headers
        )
        if user_invest_sell_stock is not None and user_invest_sell_stock['executed'] == 'N':
            user_invest_sell_stock['num_quotas'] = apply_stock['num_quotas']
            user_invest_sell_stock['avg_price_quota'] = apply_stock['avg_price_quota']
            user_invest_sell_stock['executed'] = 'S'
            self.remote_repository.update(
                "user_invest_sell_stock",
                ["id"],
                user_invest_sell_stock,
                headers
            )
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

    def re_add_resumes_period(self, args, headers):
        args_ = {}
        now = datetime.now()
        now = now - timedelta(days=1)
        args_['data_fim'] = now.strftime("%Y-%m-%d")
        args_['data_ini'] = '2021-12-01'
        args_['refazer_data_fim'] = 'S'
        args_['refazer_data_ini'] = 'N'

        if 'tipo' not in args:
            args_['tipo'] = 'all'
        else:
            args_['tipo'] = args['tipo']

        if 'data_refazer' in args:
            date_range = pandas.date_range(args_['data_ini'], args['data_refazer'])
        else:
            date_range = pandas.date_range(args['data_ini'], args_['data_fim'])

        for date in reversed(date_range):
            args_['data_ini'] = date.strftime("%Y-%m-%d")
            try:
                self.add_resumes_period(args_, headers)
            except Exception as e:
                self.logger.exception(e)
                pass

    def add_resumes_period(self, args, headers):
        args_ = {}
        for key in args:
            args_[key] = args[key]
        # check if exist arg ticker case not exist add text all
        if 'ticker' not in args_:
            args_['ticker'] = 'all'

        if 'tipo' not in args_:
            args_['tipo'] = 'all'

        if 'indice' not in args_:
            args_['indice'] = 'all'

        if 'invest_name' not in args_:
            args_['invest_name'] = 'all'

        if 'refazer_data_fim' not in args_:
            args_['refazer_data_fim'] = 'N'

        if 'refazer_data_ini' not in args_:
            args_['refazer_data_ini'] = 'S'

        # if data_inicio not in args_ add data fim as yyyy-MM-dd
        if 'data_fim' not in args_:
            now = datetime.now()
            now = now - timedelta(days=1)
            args_['data_fim'] = now.strftime("%Y-%m-%d")

        # if data_ini not in args_ add data ini as 2022-01-01
        if 'data_ini' not in args_:
            args_['data_ini'] = '2021-12-01'
        if args_['tipo'] == 'all':
            tipos = ['tipo', 'ativo']
        else:
            tipos = [args_['tipo']]
        for tipo in tipos:
            if tipo == 'ativo':
                args_['indice'] = 'nenhum'
            args_['tipo'] = tipo
            date_range = pandas.date_range(args_['data_ini'], args_['data_fim'])
            data_ini_ = args_['data_ini']
            data_fim_ = args_['data_fim']

            args_['data_ini'] = data_ini_
            if args_['refazer_data_fim'] == 'S':
                msg_ = f"O resumo do período {data_ini_} até {data_fim_} foi solicitado. Os argumentos são: {args_}"
                Utils.inform_to_client(args_, "Resumo por período solicitado", headers, msg_)
                for date in date_range:
                    self.logger.info(tipo + " data ini -> " + data_ini_ + " data fim -> " + date.strftime("%Y-%m-%d"))
                    if date.strftime("%Y-%m-%d") > data_ini_ and date.strftime("%Y-%m-%d") < data_fim_:
                        args_['data_fim'] = date.strftime("%Y-%m-%d")
                        try:
                            self.get_resume_invest(args_, headers)
                        except Exception as e:
                            self.logger.exception(e)
                            pass

            if args_['refazer_data_ini'] == 'S':
                for date in date_range:
                    self.logger.info(tipo + " data ini -> " + date.strftime("%Y-%m-%d"))
                    self.logger.info(tipo + " data ini -> " + date.strftime("%Y-%m-%d") + " data fim -> " + data_fim_)
                    if date.strftime("%Y-%m-%d") > data_ini_ and date.strftime("%Y-%m-%d") < data_fim_:
                        args_['data_ini'] = date.strftime("%Y-%m-%d")
                        try:
                            self.get_resume_invest(args_, headers)
                        except Exception as e:
                            self.logger.exception(e)
                            pass
            args_['data_ini'] = data_ini_
            args_['data_fim'] = data_fim_
            resume_calculed = {
                "data_ini": args_['data_ini'],
                "data_fim": args_['data_fim'],
                "type": args_['tipo'],
            }
            self.remote_repository.insert("user_resume_calculed", resume_calculed, headers)
            msg_ = f"O resumo do período {data_ini_} até {data_fim_} foi adicionado com sucesso. Os argumentos foram: {args_}"
            Utils.inform_to_client(args_, "Resumo por perído finalizado", headers, msg_)

    def get_resume_invest_grafic(self, args, headers):
        select = Utils.read_file("static/resume_grafic.sql")
        args_ = {}
        for key in args:
            args_[key] = args[key]
        # check if exist arg ticker case not exist add text all
        if 'ticker' not in args_:
            args_['ticker'] = 'all'

        if 'tipo' not in args_:
            args_['tipo'] = 'all'

        if 'date_mask' not in args_:
            args_['date_mask'] = 'YYYY-MM'

        if 'indice' not in args_:
            args_['indice'] = 'all'

        if 'invest_name' not in args_:
            args_['invest_name'] = 'all'

        # if data_inicio not in args_ add data fim as yyyy-MM-dd
        if 'data_fim' not in args_:
            now = datetime.now()
            now = now + timedelta(days=1)
            args_['data_fim'] = now.strftime("%Y-%m-%d")

        # if data_ini not in args_ add data ini as 2022-01-01
        if 'data_ini' not in args_:
            args_['data_ini'] = '2021-12-01'

        for key in args_:
            if key == 'ticker':
                names = args_[key].split(',')
                names_ = ""
                for name in names:
                    names_ = names_ + "'" + name + "',"
                names_ = names_[:-1]
                select = select.replace(":" + key, names_)
            elif key == 'sorted_by':
                select = select.replace(":" + key, args_[key])
            else:
                select = select.replace(":" + key, "'" + args_[key] + "'")
        result_ = self.remote_repository.execute_select(select, headers)

        dates = []
        labels = []
        serie = {}
        for res_ in result_:
            if res_['date_time'] not in dates:
                dates.append(res_['date_time'])
            if res_['label'] not in labels:
                labels.append(res_['label'])
            serie[res_['date_time'] + res_['label']] = res_['value']
        for date in dates:
            for label in labels:
                if date + label not in serie:
                    serie[date + label] = 0
        series = []
        for label in labels:
            serie_ = {'name': label, 'data': []}
            for date in dates:
                serie_['data'].append(serie[date + label])
            series.append(serie_)
        result_ = {
            "categories": dates,
            "series": series
        }
        return result_

    def get_resume_invest(self, args, headers):
        select = Utils.read_file("static/Resume.sql")
        args_ = {}
        for key in args:
            args_[key] = args[key]
        # check if exist arg ticker case not exist add text all
        if 'ticker' not in args_:
            args_['ticker'] = 'all'

        if 'tipo' not in args_:
            args_['tipo'] = 'all'

        if 'indice' not in args_:
            args_['indice'] = 'all'

        if 'invest_name' not in args_:
            args_['invest_name'] = 'all'

        # if data_inicio not in args_ add data fim as yyyy-MM-dd
        if 'data_fim' not in args_:
            now = datetime.now()
            now = now + timedelta(days=1)
            args_['data_fim'] = now.strftime("%Y-%m-%d")

        # if data_ini not in args_ add data ini as 2022-01-01
        if 'data_ini' not in args_:
            args_['data_ini'] = '2021-12-01'
        data_ini_ = args_['data_ini']
        args_['data_ini'] = data_ini_ + " 23:59:59.999"
        data_fim_ = args_['data_fim']
        args_['data_fim'] = data_fim_ + " 23:59:59.999"
        for key in args_:
            select = select.replace(":" + key, "'" + args_[key] + "'")
        result_ = self.remote_repository.execute_select(select, headers)
        results_save_ = []
        for res_ in result_:
            result_save_ = res_
            result_save_['data_ini'] = data_ini_
            result_save_['data_fim'] = data_fim_
            results_save_.append(result_save_)
        self.remote_repository.insert("user_resume_values", results_save_, headers)
        if args_['tipo'] != 'carteira':
            args_carteira_ = {}
            args_carteira_['ticker'] = 'all'
            args_carteira_['tipo'] = 'carteira'
            args_carteira_['indice'] = 'nenhum'
            args_carteira_['data_fim'] = data_fim_
            args_carteira_['data_ini'] = data_ini_
            args_carteira_['invest_name'] = 'all'
            result_carteira_ = self.get_resume_invest(args_carteira_, headers)
            result_ = result_ + result_carteira_
        if 'sentido' in args_:
            reverse_ = args_['sentido'] == 'desc'
        else:
            reverse_ = True
        if 'sorted_by' in args:
            result_ = sorted(result_, key=lambda k: k[args_['sorted_by']], reverse=reverse_)
        else:
            result_ = sorted(result_, key=lambda k: k['ganho_mensalizado_medio'], reverse=reverse_)
        return result_

    def load_prices(self, ticker, type, name=None, headers=None, data_=None):
        if name is None:
            name = ticker
        stock_ = self.remote_repository.get_object("stocks", ["ticker"], {"ticker": ticker}, headers)
        infos = self.http_repository.get_prices(name, type, False, "4")
        for info in infos:
            prices = info['prices']
            for price in prices:
                stock_['price'] = price['price']
                data = datetime.strptime(price['date'], '%d/%m/%y %H:%M').strftime("%Y-%m-%d")
                can_insert = True
                if data_ is not None:
                    can_insert = data > data_
                if can_insert:
                    self.add_stock_price(stock_, headers, data)
