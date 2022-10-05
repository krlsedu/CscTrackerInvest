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
        print("Atualizando bdrs")
        self.att_brd_expres()
        print("Atualizando ranks")
        investment_handler.att_stocks_ranks()
        print("end att express")

    def att_full(self):
        print("Atualizando fiis")
        self.att_fiis(True)
        print("Atualizando acoes")
        self.att_acoes(True)
        print("Atualizando fundos")
        self.att_fundos()
        print("Atualizando BDRs")
        self.att_bdr()
        print("Atualizando ranks")
        investment_handler.att_stocks_ranks()
        print("end att full")

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

            generic_repository.update("stocks", ["ticker"], stock_)

            investment_handler.add_stock_price(stock_)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return bdrs

    def att_brd_expres(self):
        bdrs = self.load_bdr_used()
        for bdr in bdrs:
            company_ = bdr['ticker']
            stock_ = investment_handler.get_stock(company_)
            stock_, investment_type = http_repository.get_values_by_ticker(stock_, True)

            generic_repository.update("stocks", ["ticker"], stock_)

            investment_handler.add_stock_price(stock_)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return bdrs

    def load_bdr_used(self):
        bdrs = generic_repository.get_objects_from_sql(
            "select * from stocks "
            "where investment_type_id = 4 "
            "   and exists( select 1 from user_stocks where user_stocks.investment_id = stocks.id)")
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
            stock_, investment_type = http_repository.get_values_by_ticker(fundo, True)

            generic_repository.update("stocks", ["ticker"], stock_)

            stock_['price'] = float(stock_['price'])
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
        investment_handler.att_stock_price_new(daily, stock, stock_, type)

    def update_stock(self, stock):
        if stock['investment_type'] == 2:
            self.att_price_generic(stock, 'fii')
        elif stock['investment_type'] == 1:
            self.att_price_generic(stock, 'acao')
        elif stock['investment_type'] == 4:
            self.att_price_generic(stock, 'bdr')
        return stock
