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

    def att_all(self):
        tp = request.args.get('tp_invest')
        if tp is None:
            self.att_bdr()
            self.att_fiis()
            self.att_acoes()
        elif tp == 'fiis':
            self.att_fiis()
        elif tp == 'acoes':
            self.att_acoes()
        elif tp == 'bdr':
            self.att_bdr()
        investment_handler.att_stocks_ranks()

    def att_acoes(self):
        acoes = load_acoes_info()
        for acao in acoes:
            print(f"Atualizando a acao: {acao['ticker']}")
            stock_ = investment_handler.get_stock(acao['ticker'])
            stock_, investment_type = http_repository.get_values_by_ticker(stock_, True)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return acoes

    def att_bdr(self):
        bdrs = load_bdr_info()
        for bdr in bdrs:
            company_ = bdr['url']
            company_ = company_.replace('/bdrs/', '')
            print(f"Atualizando BDR: {company_}")
            stock_ = investment_handler.get_stock(company_)
            stock_, investment_type = http_repository.get_values_by_ticker(stock_, )
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return bdrs

    def att_fiis(self):
        fiis = load_fiis_info()
        for fii in fiis:
            print(f"Atualizando o fundo: {fii['ticker']}")
            stock_ = investment_handler.get_stock(fii['ticker'])
            stock_, investment_type = http_repository.get_values_by_ticker(stock_, True)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return fiis
