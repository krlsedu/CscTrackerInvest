from repository.HttpRepository import HttpRepository
from repository.Repository import GenericRepository
from service.Interceptor import Interceptor
from service.InvestmentHandler import InvestmentHandler
from service.LoadInfo import load_fiis_info, load_acoes_info

investment_handler = InvestmentHandler()
generic_repository = GenericRepository()
http_repository = HttpRepository()


class AttStocks(Interceptor):
    def __init__(self):
        super().__init__()

    def att_all(self):
        self.att_fiis()
        self.att_acoes()
        investment_handler.att_stocks_ranks()

    def att_acoes(self):
        acoes = load_acoes_info()
        for acao in acoes:
            print(f"Atualizando a acao: {acao['ticker']}")
            stock_ = investment_handler.get_stock(acao['ticker'])
            stock_, investment_type = http_repository.get_values_by_ticker(stock_)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return acoes

    def att_fiis(self):
        fiis = load_fiis_info()
        for fii in fiis:
            print(f"Atualizando o fundo: {fii['ticker']}")
            stock_ = investment_handler.get_stock(fii['ticker'])
            stock_, investment_type = http_repository.get_values_by_ticker(stock_)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return fiis
