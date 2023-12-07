import decimal
import json
from datetime import datetime

from repository.HttpRepository import HttpRepository
from service.FixedIncome import FixedIncome
from service.Interceptor import Interceptor
from service.InvestmentHandler import InvestmentHandler
from service.LoadInfo import load_fiis_info, load_acoes_info, load_bdr_info, load_indices

investment_handler = InvestmentHandler()
http_repository = HttpRepository()
fixed_income_handler = FixedIncome()


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)


class AttStocks(Interceptor):
    def __init__(self):
        super().__init__()

    def att_expres(self, headers=None):
        print("Atualizando indices")
        self.att_indices(headers)
        print("Atualizando fiis express")
        self.att_fiis(headers, False, True)
        print("Atualizando acoes express")
        self.att_acoes(headers, False, True)
        print("Atualizando Criptos")
        self.att_criptos(headers)
        print("Atualizando fundos")
        self.att_fundos(headers)
        print("Atualizando bdrs")
        self.att_brd_expres(headers)
        print("Atualizando ranks")
        investment_handler.att_stocks_ranks(headers)
        print("Atualizando fiis")
        self.att_fiis(headers)
        print("Atualizando acoes")
        self.att_acoes(headers)
        print("Atualizando Criptos")
        self.att_criptos(headers)
        print("Atualizando fundos")
        self.att_fundos(headers)
        print("Atualizando bdrs")
        self.att_brd_expres(headers)
        investment_handler.att_stocks_ranks(headers)
        print("end att express")

    def att_full(self, headers=None):
        print("Atualizando fiis")
        self.att_fiis(headers, True)
        print("Atualizando acoes")
        self.att_acoes(headers, True)
        print("Atualizando Criptos")
        self.att_criptos(headers)
        print("Atualizando fundos")
        self.att_fundos(headers)
        print("Atualizando BDRs")
        self.att_bdr(headers)
        print("Atualizando ranks")
        investment_handler.att_stocks_ranks(headers)
        print("end att full")

    def att_acoes(self, headers=None, full=False, express=False):
        if express:
            acoes = self.load_my_invest(headers, 1)
        else:
            acoes = load_acoes_info()
        count_ = 0
        for acao in acoes:
            count_ += 1
            try:
                print(f"Atualizando a acao: {acao['ticker']} - {count_}/{len(acoes)}")
                stock_ = investment_handler.get_stock(acao['ticker'], headers)

                if full:
                    stock_, investment_type, _ = http_repository.get_values_by_ticker(stock_, True, headers)
                else:
                    try:
                        stock_['price'] = acao['price']
                        stock_['pl'] = acao['p_L']
                        stock_['ev_ebit'] = acao['eV_Ebit']
                        stock_['avg_liquidity'] = acao['liquidezMediaDiaria']
                        stock_['dy'] = acao['dy']
                    except Exception as e:
                        pass

                stock_ = investment_handler.att_dividend_info(stock_, headers)

                investment_handler.att_price_yahoo(stock_, headers)

                print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
            except Exception as e:
                print(f"Erro ao atualizar {acao['ticker']} - {e}")
        return acoes

    def att_criptos(self, headers=None, full=False):
        acoes = http_repository.get_objects("stocks", ["investment_type_id"], {"investment_type_id": 100}, headers)
        for acao in acoes:
            try:
                print(f"Atualizando cripto: {acao['ticker']}")
                stock_ = investment_handler.get_stock(acao['ticker'], headers)

                stock_ = investment_handler.att_dividend_info(stock_, headers)

                investment_handler.att_price_yahoo_us(stock_, headers)

                print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
            except Exception as e:
                print(f"Erro ao atualizar {acao['ticker']} - {e}")
        return acoes

    def att_bdrs(self, headers=None):
        self.att_bdr(headers)
        investment_handler.att_stocks_ranks(headers)

    def att_bdr(self, headers=None):
        bdrs = load_bdr_info()
        count_ = 0
        for bdr in bdrs:
            count_ += 1
            try:
                company_ = bdr['url']
                company_ = company_.replace('/bdrs/', '')
                print(f"Atualizando BDR: {company_} - {count_}/{len(bdrs)}")
                stock_ = investment_handler.get_stock(company_, headers)
                stock_, _, _ = http_repository.get_values_by_ticker(stock_, True, headers)

                stock_ = investment_handler.att_dividend_info(stock_, headers)

                investment_handler.att_info_yahoo(stock_, headers)

                print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
            except Exception as e:
                print(f"Erro ao atualizar {bdr['url']} - {e}")
        return bdrs

    def att_brd_expres(self, headers=None):
        bdrs = self.load_bdr_used(headers)
        for bdr in bdrs:
            try:
                company_ = bdr['ticker']
                stock_ = investment_handler.get_stock(company_, headers)
                stock_, investment_type, _ = http_repository.get_values_by_ticker(stock_, True, headers)

                stock_ = investment_handler.att_dividend_info(stock_, headers)

                investment_handler.att_price_yahoo(stock_, headers)
                print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
            except Exception as e:
                print(f"Erro ao atualizar {bdr['ticker']} - {bdr['name']} - {e}")
        return bdrs

    def load_bdr_used(self, headers=None):
        bdrs = http_repository.execute_select(
            "select * from stocks "
            "where investment_type_id = 4 "
            "   and exists( select 1 from user_stocks where user_stocks.investment_id = stocks.id and quantity > 0)",
            headers)
        return bdrs

    def load_my_invest(self, headers, investment_type_id):
        keys = ['ticker', 'price', 'dy', 'last_dividend', 'pvp', 'segment', 'name', 'investment_type_id']
        ks = str(keys).replace("[", "").replace("]", "").replace("'", "")
        bdrs = http_repository.execute_select(
            f"select {ks} from stocks "
            f"where investment_type_id = {investment_type_id} "
            "   and exists( select 1 from user_stocks where user_stocks.investment_id = stocks.id and quantity > 0)",
            headers)
        return bdrs

    def att_fiis(self, headers, full=False, express=False):
        if express:
            fiis = self.load_my_invest(headers, 2)
        else:
            fiis = load_fiis_info(headers)
        count_ = 0
        for fii in fiis:
            count_ += 1
            print(f"Atualizando o fundo: {fii['ticker']} - {count_}/{len(fiis)}")
            try:
                stock_ = investment_handler.get_stock(fii['ticker'], headers)
                if full:
                    stock_, investment_type, _ = http_repository.get_values_by_ticker(stock_, True, headers)
                else:
                    try:
                        stock_['price'] = fii['price']
                        stock_['dy'] = fii['dy']
                        stock_['last_dividend'] = fii['lastdividend']
                        stock_['avg_liquidity'] = fii['liquidezmediadiaria']
                    except Exception as e:
                        print("Erro ao atualizar o fundo: " + fii['ticker'] + " - " + str(e))
                        pass

                stock_ = investment_handler.att_dividend_info(stock_, headers)

                investment_handler.att_price_yahoo(stock_, headers)

                print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
            except Exception as e:
                print("Erro ao atualizar o fundo: " + fii['ticker'] + " - " + str(e))
        return fiis

    def att_fundos(self, headers=None):
        fundos = http_repository.get_objects("stocks", ["investment_type_id"], {"investment_type_id": 15}, headers)
        for fundo in fundos:
            try:
                stock_, investment_type, _ = http_repository.get_values_by_ticker(fundo, True, headers)

                stock_ = investment_handler.att_dividend_info(stock_, headers)

                http_repository.update("stocks", ["ticker"], stock_, headers)

                print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
            except Exception as e:
                print(f"Erro ao atualizar {fundo['ticker']} - {fundo['name']} - {e}")
        return fundos

    def att_prices(self, headers, daily=False):
        if not daily:
            fundos = http_repository.get_objects("stocks", ["investment_type_id"], {"investment_type_id": 15}, headers)
            self.att_prices_generic(headers, fundos, 'fundo', daily)
        fiis = load_fiis_info(headers)
        self.att_prices_generic(headers, fiis, 'fii', daily)
        acoes = load_acoes_info()
        self.att_prices_generic(headers, acoes, 'acao', daily)
        bdrs = load_bdr_info()
        self.att_prices_generic(headers, bdrs, 'bdr', daily)

    def att_dividends_info(self, headers):
        my_stocks = http_repository.get_objects("stocks", [], {}, headers)
        counter = 0
        for stock in my_stocks:
            counter += 1
            stock_ = stock

            print(f"Atualizando mapa de dividendos de {stock_['ticker']} - {stock_['name']} - "
                  f"{counter}/{len(my_stocks)}")
            self.dividends_map_info(headers, stock_)

    def att_user_dividends_info(self, headers):
        my_stocks = http_repository.get_objects("user_stocks", [], {}, headers)
        counter = 0
        for stock in my_stocks:
            counter += 1
            stock_ = http_repository.get_object("stocks", ["id"], {"id": stock['investment_id']}, headers)
            # generate log of progress of stock processing with overal progress
            print(f"Atualizando mapa de dividendos de {stock_['ticker']} - {stock_['name']} - "
                  f"{counter}/{len(my_stocks)}")
            if (stock_['investment_type_id'] == 1
                    or stock_['investment_type_id'] == 2
                    or stock_['investment_type_id'] == 4):
                self.dividends_map_info(headers, stock_)

        self.dividends_info(headers)

    def dividends_info(self, headers):
        my_stocks = http_repository.get_objects("user_stocks", [], {}, headers)
        for stock in my_stocks:
            stock_ = http_repository.get_object("stocks", ["id"], {"id": stock['investment_id']}, headers)
            dividends_map = http_repository.get_objects("dividends_map", ["investment_id"],
                                                        {"investment_id": stock_['id']}, headers)
            for dividend_map in dividends_map:
                try:
                    value_per_quote = dividend_map['value_per_quote']
                    if dividend_map['type_id'] == 2:
                        value_per_quote = value_per_quote * 0.85

                    select = f"select " \
                             f"coalesce(sum(case when movement_type = 1 then quantity else -quantity end),0) as quantity " \
                             f"from user_stocks_movements where investment_id = {dividend_map['investment_id']} " \
                             f"and date <= '{dividend_map['date_with']} 23:59:59'"
                    quantity = http_repository.execute_select(select, headers)[0]['quantity']
                    if quantity > 0:
                        dividend = {"dividends_map_id": dividend_map['id']}
                        dividend = http_repository.get_object("dividends", ["dividends_map_id"],
                                                              dividend, headers)
                        if dividend is None:
                            dividend = {"investment_id": stock_['id'],
                                        "dividends_map_id": dividend_map['id'],
                                        "date_payment": dividend_map['date_payment'],
                                        "value_per_quote": value_per_quote,
                                        "type_id": dividend_map['type_id'],
                                        "active": "S",
                                        "quantity": quantity,
                                        }
                            http_repository.insert("dividends", dividend, headers)
                        else:
                            dividend['value_per_quote'] = value_per_quote
                            dividend['quantity'] = quantity
                            dividend['date_payment'] = dividend_map['date_payment']
                            http_repository.update("dividends", ["id"], dividend, headers)
                except Exception as e:
                    print("Erro ao atualizar dividendos de " + stock_['ticker'])
                    print(e)
                    pass

    def dividends_map_info(self, headers, stock_):
        if stock_['investment_type_id'] == 1 or \
                stock_['investment_type_id'] == 4 or \
                stock_['investment_type_id'] == 2:
            url = None
            status_invest = True
            if stock_['investment_type_id'] == 1:
                # url = f"https://statusinvest.com.br/acoes/{stock_['ticker']}"
                url = f"https://investidor10.com.br/acoes/{stock_['ticker']}"
                status_invest = False
            elif stock_['investment_type_id'] == 4:
                url = f"https://statusinvest.com.br/bdrs/{stock_['ticker']}"
            elif stock_['investment_type_id'] == 2:
                # url = f"https://statusinvest.com.br/fundos-imobiliarios/{stock_['ticker']}"
                url = f"https://investidor10.com.br/fiis/{stock_['ticker']}"
                status_invest = False
            try:
                soup = http_repository.get_soup(url, headers)
                table = None
                if status_invest:
                    table = soup.find('div', {'id': 'earning-section'})
                else:
                    table = soup.find('table', {'id': 'table-dividends-history'})
                if status_invest:
                    rows = table.find('input', {'id': 'results'})
                    rows = rows.get('value')
                    rows = json.loads(rows)
                else:
                    rows = table.find_all('tr')

                for row in rows:
                    finded = False
                    date_with = None
                    date_pay = None
                    value = None
                    _type = None
                    if status_invest:
                        value = row['v']
                        date_with = row['ed']
                        date_pay = row['pd']
                        _type = row['et']
                        finded = True
                    else:
                        cells = row.find_all('td')
                        if len(cells) > 0:
                            _type = cells[0].text
                            date_with = cells[1].text
                            date_pay = cells[2].text
                            value = cells[3].text
                            value = self.convert_pt_br_number_to_db_number(value)
                            finded = True
                    if finded:
                        if _type == 'Rendimento' or _type == 'REND. TRIBUTADO' or (
                                _type == 'Dividendos' and stock_['investment_type_id'] == 2):
                            _type = 3
                        elif _type == 'Juros sobre Capital Próprio' or _type == 'JSCP':
                            _type = 2
                        else:
                            _type = 1
                        date_with = self.convert_pt_br_date_to_db_date(date_with)
                        date_pay = self.convert_pt_br_date_to_db_date(date_pay)
                        dividend_map = {"investment_id": stock_['id'],
                                        "date_with": date_with,
                                        "date_payment": date_pay,
                                        "type_id": _type}
                        dividend_map = http_repository.get_object("dividends_map", ["investment_id", "date_with",
                                                                                    "date_payment", "type_id"],
                                                                  dividend_map, headers)
                        if dividend_map is None:
                            dividend_map = {"investment_id": stock_['id'],
                                            "date_with": date_with,
                                            "date_payment": date_pay,
                                            "type_id": _type,
                                            "value_per_quote": value}
                            http_repository.insert("dividends_map", dividend_map, headers)
                        else:
                            dividend_map['value_per_quote'] = value
                            http_repository.update("dividends_map", ["id"],
                                                   dividend_map, headers)

            except Exception as e:
                print("Erro ao atualizar dividendos de " + stock_['ticker'])
                print(e)
                pass

    def convert_pt_br_number_to_db_number(self, number):
        if number is None or number == '':
            return None
        number = number.replace('.', '')
        number = number.replace(',', '.')
        return number

    def convert_pt_br_date_to_db_date(self, date):
        if date is None or date == '':
            return None
        date = date.split('/')
        return date[2] + '-' + date[1] + '-' + date[0]

    def att_prices_generic(self, headers, stocks, type, daily=False):
        for stock in stocks:
            self.att_price_generic(headers, stock, type, daily)

    def att_price_generic(self, headers, stock, type, daily=False):
        if type == 'bdr':
            company_ = stock['url']
            company_ = company_.replace('/bdrs/', '')
            print(f"Atualizando o {type}: {company_}")
            stock_ = investment_handler.get_stock(company_, headers)
            stock['price'] = stock_['price']
        elif type == 'fundo':
            stock_ = stock
        else:
            print(f"Atualizando a {type}: {stock['ticker']}")
            stock_ = investment_handler.get_stock(stock['ticker'], headers)
            stock['price'] = stock_['price']
        if type == 'fundo':
            investment_handler.att_stock_price_new(headers, daily, stock, stock_, type)
        else:
            investment_handler.att_prices_yahoo(stock_, headers, "1d", "15m")

    def update_stock(self, headers, stock):
        if stock['investment_type'] == 2:
            self.att_price_generic(headers, stock, 'fii')
        elif stock['investment_type'] == 1:
            self.att_price_generic(headers, stock, 'acao')
        elif stock['investment_type'] == 4:
            self.att_price_generic(headers, stock, 'bdr')
        return stock

    def att_indices(self, headers):
        fiis = load_indices(headers)
        count_ = 0
        for fii in fiis:
            count_ += 1
            print(f"Atualizando o índice: {fii['ticker']} - {count_}/{len(fiis)}")
            try:
                stock_ = investment_handler.get_stock(fii['ticker'], headers)
                if stock_['tx_type'] is None:
                    investment_handler.att_price_yahoo(stock_, headers)
                    stock_['price'] = http_repository.get_info(stock_, "cotacao", headers)
                    investment_handler.add_stock_price(stock_, headers)
                    http_repository.update("stocks", ["ticker"], stock_, headers)
                else:
                    movement = {
                        "buy_date": datetime.now().strftime('%Y-%m-%d'),
                        "ticker": stock_['ticker']
                    }
                    fixed_income_handler.get_stock_price(movement, headers)

                print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
            except Exception as e:
                print("Erro ao atualizar o fundo: " + fii['ticker'] + " - " + str(e))
        return fiis
