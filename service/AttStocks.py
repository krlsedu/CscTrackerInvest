from datetime import datetime

from flask import request

from repository.HttpRepository import HttpRepository
from repository.Repository import GenericRepository
from service.Interceptor import Interceptor
from service.InvestmentHandler import InvestmentHandler
from service.LoadInfo import load_fiis_info, load_acoes_info, load_bdr_info

investment_handler = InvestmentHandler()
generic_repository = GenericRepository()
http_repository = HttpRepository()


class AttStocks(Interceptor):
    def __init__(self):
        super().__init__()

    def att_expres(self):
        print("Atualizando fiis")
        self.att_fiis()
        print("Atualizando acoes")
        self.att_acoes()
        print("Atualizando fundos")
        self.att_fundos()
        print("Atualizando ranks")
        investment_handler.att_stocks_ranks()

    def att_all(self):
        tp = request.args.get('tp_invest')
        if tp is None:
            self.att_fiis()
            self.att_acoes()
            investment_handler.att_stocks_ranks()
            self.att_bdr()
        elif tp == 'fiis':
            self.att_fiis()
        elif tp == 'acoes':
            self.att_acoes()
        elif tp == 'bdr':
            self.att_bdr()
        elif tp == 'express':
            self.att_fiis()
            self.att_acoes()
        investment_handler.att_stocks_ranks()

    def att_acoes(self, full=False):
        acoes = load_acoes_info()
        for acao in acoes:
            print(f"Atualizando a acao: {acao['ticker']}")
            stock_ = investment_handler.get_stock(acao['ticker'])

            if full:
                stock_, investment_type = http_repository.get_values_by_ticker(stock_, True)
            else:
                try:
                    stock_['price'] = acao['price']
                    stock_['pvp'] = acao['p_VP']
                    stock_['pl'] = acao['p_L']
                    stock_['ev_ebit'] = acao['eV_Ebit']
                    stock_['avg_liquidity'] = acao['liquidezMediaDiaria']
                    stock_['dy'] = acao['dy']
                except Exception as e:
                    pass
            generic_repository.update("stocks", ["ticker"], stock_)

            investment_handler.add_stock_price(stock_)

            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return acoes

    def att_bdrs(self):
        self.att_bdr()
        investment_handler.att_stocks_ranks()

    def att_bdr(self):
        bdrs = load_bdr_info()
        for bdr in bdrs:
            company_ = bdr['url']
            company_ = company_.replace('/bdrs/', '')
            print(f"Atualizando BDR: {company_}")
            stock_ = investment_handler.get_stock(company_)
            stock_, investment_type = http_repository.get_values_by_ticker(stock_, True)

            investment_handler.add_stock_price(stock_)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return bdrs

    def att_fiis(self, full=False):
        fiis = load_fiis_info()
        for fii in fiis:
            print(f"Atualizando o fundo: {fii['ticker']}")
            stock_ = investment_handler.get_stock(fii['ticker'])
            if full:
                stock_, investment_type = http_repository.get_values_by_ticker(stock_, True)
            else:
                try:
                    stock_['price'] = fii['price']
                    stock_['dy'] = fii['dy']
                    stock_['pvp'] = fii['p_vp']
                    stock_['last_dividend'] = fii['lastdividend']
                    stock_['avg_liquidity'] = fii['liquidezmediadiaria']
                except Exception as e:
                    pass
            generic_repository.update("stocks", ["ticker"], stock_)

            investment_handler.add_stock_price(stock_)

            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return fiis

    def att_fundos(self):
        fundos = generic_repository.get_objects("stocks", ["investment_type_id"], {"investment_type_id": 15})
        for fundo in fundos:
            company_ = fundo['url_infos']
            company_ = company_.replace('/fundos-de-investimento/', '')
            print(f"Atualizando Fundo: {company_}")
            stock_ = investment_handler.get_stock(company_)
            stock_, investment_type = http_repository.get_values_by_ticker(stock_, True)

            investment_handler.add_stock_price(stock_)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return fundos

    def att_prices(self, daily=False):
        if not daily:
            fundos = generic_repository.get_objects("stocks", ["investment_type_id"], {"investment_type_id": 15})
            self.att_prices_generic(fundos, 'fundo', daily)
        fiis = load_fiis_info()
        self.att_prices_generic(fiis, 'fii', daily)
        acoes = load_acoes_info()
        self.att_prices_generic(acoes, 'acao', daily)
        bdrs = load_bdr_info()
        self.att_prices_generic(bdrs, 'bdr', daily)

    def att_prices_generic(self, stocks, type, daily=False):
        for stock in stocks:
            self.att_price_generic(stock, type, daily)

    def att_price_generic(self, stock, type, daily=False):
        if type == 'bdr':
            company_ = stock['url']
            company_ = company_.replace('/bdrs/', '')
            print(f"Atualizando o {type}: {company_}")
            stock_ = investment_handler.get_stock(company_)
            stock['price'] = stock_['price']
        elif type == 'fundo':
            stock_ = stock
        else:
            print(f"Atualizando a {type}: {stock['ticker']}")
            stock_ = investment_handler.get_stock(stock['ticker'])
            stock['price'] = stock_['price']
        if stock_['prices_imported'] == 'N' or daily:
            if type == 'fundo' and not daily:
                company_ = stock['url_infos']
                company_ = company_.replace('/fundos-de-investimento/', '')
                infos = http_repository.get_prices_fundos(company_)
                datas = infos['data']['chart']['category']
                values = infos['data']['chart']['series']['fundo']
                for i in range(len(datas)):
                    data = datas[i]
                    data = datetime.strptime(data, '%d/%m/%y').strftime("%Y-%m-%d")
                    price = values[i]['price']
                    stock_['price'] = price
                    investment_handler.add_stock_price(stock_, data)
                pass
            else:
                infos = http_repository.get_prices(stock_['ticker'], type, daily)
                for info in infos:
                    prices = info['prices']
                    for price in prices:
                        stock_['price'] = price['price']
                        if daily:
                            investment_handler.add_stock_price(stock_,
                                                               datetime.strptime(price['date'], '%d/%m/%y %H:%M')
                                                               .strftime("%Y-%m-%d %H:%M"))
                        else:
                            investment_handler.add_stock_price(stock_,
                                                               datetime.strptime(price['date'], '%d/%m/%y %H:%M')
                                                               .strftime("%Y-%m-%d"))
            stock_['prices_imported'] = 'S'
            stock_['price'] = stock['price']
            generic_repository.update("stocks", ["ticker"], stock_)
        print(f"{stock_['ticker']} - {stock_['name']} - atualizado")

    def update_stock(self, stock):
        if stock['investment_type'] == 2:
            self.att_price_generic(stock, 'fii')
        elif stock['investment_type'] == 1:
            self.att_price_generic(stock, 'acao')
        elif stock['investment_type'] == 4:
            self.att_price_generic(stock, 'bdr')
        return stock
