from repository.HttpRepository import HttpRepository
from service.Interceptor import Interceptor
from service.InvestmentHandler import InvestmentHandler
from service.LoadInfo import load_fiis_info, load_acoes_info, load_bdr_info

investment_handler = InvestmentHandler()
http_repository = HttpRepository()

class AttStocks(Interceptor):
    def __init__(self):
        super().__init__()

    def att_expres(self, headers=None):
        print("Atualizando fiis")
        self.att_fiis(headers)
        print("Atualizando acoes")
        self.att_acoes(headers)
        print("Atualizando fundos")
        self.att_fundos(headers)
        print("Atualizando bdrs")
        self.att_brd_expres(headers)
        print("Atualizando ranks")
        investment_handler.att_stocks_ranks(headers)
        print("end att express")

    def att_full(self, headers=None):
        print("Atualizando fiis")
        self.att_fiis(headers, True)
        print("Atualizando acoes")
        self.att_acoes(headers, True)
        print("Atualizando fundos")
        self.att_fundos(headers)
        print("Atualizando BDRs")
        self.att_bdr(headers)
        print("Atualizando ranks")
        investment_handler.att_stocks_ranks(headers)
        print("end att full")

    def att_acoes(self, headers=None, full=False):
        acoes = load_acoes_info()
        for acao in acoes:
            print(f"Atualizando a acao: {acao['ticker']}")
            stock_ = investment_handler.get_stock(acao['ticker'], headers)

            if full:
                stock_, investment_type = http_repository.get_values_by_ticker(stock_, True, headers)
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
            http_repository.update("stocks", ["ticker"], stock_, headers)

            investment_handler.add_stock_price(stock_, headers)

            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return acoes

    def att_bdrs(self, headers=None):
        self.att_bdr(headers)
        investment_handler.att_stocks_ranks(headers)

    def att_bdr(self, headers=None):
        bdrs = load_bdr_info()
        for bdr in bdrs:
            company_ = bdr['url']
            company_ = company_.replace('/bdrs/', '')
            print(f"Atualizando BDR: {company_}")
            stock_ = investment_handler.get_stock(company_, headers)
            stock_, investment_type = http_repository.get_values_by_ticker(stock_, True, headers)

            http_repository.update("stocks", ["ticker"], stock_, headers)

            investment_handler.add_stock_price(stock_, headers)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return bdrs

    def att_brd_expres(self, headers=None):
        bdrs = self.load_bdr_used(headers)
        for bdr in bdrs:
            company_ = bdr['ticker']
            stock_ = investment_handler.get_stock(company_, headers)
            stock_, investment_type = http_repository.get_values_by_ticker(stock_, True, headers)

            http_repository.update("stocks", ["ticker"], stock_, headers)

            investment_handler.add_stock_price(stock_, headers)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return bdrs

    def load_bdr_used(self, headers=None):
        bdrs = http_repository.execute_select(
            "select * from stocks "
            "where investment_type_id = 4 "
            "   and exists( select 1 from user_stocks where user_stocks.investment_id = stocks.id)", headers)
        return bdrs

    def att_fiis(self, headers, full=False):
        fiis = load_fiis_info()
        for fii in fiis:
            print(f"Atualizando o fundo: {fii['ticker']}")
            stock_ = investment_handler.get_stock(fii['ticker'], headers)
            if full:
                stock_, investment_type = http_repository.get_values_by_ticker(stock_, True, headers)
            else:
                try:
                    stock_['price'] = fii['price']
                    stock_['dy'] = fii['dy']
                    stock_['pvp'] = fii['p_vp']
                    stock_['last_dividend'] = fii['lastdividend']
                    stock_['avg_liquidity'] = fii['liquidezmediadiaria']
                except Exception as e:
                    pass

            http_repository.update("stocks", ["ticker"], stock_, headers)

            investment_handler.add_stock_price(stock_, headers)

            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return fiis

    def att_fundos(self, headers=None):
        fundos = http_repository.get_objects("stocks", ["investment_type_id"], {"investment_type_id": 15}, headers)
        for fundo in fundos:
            stock_, investment_type = http_repository.get_values_by_ticker(fundo, True, headers)

            http_repository.update("stocks", ["ticker"], stock_, headers)

            stock_['price'] = float(stock_['price'])
            investment_handler.add_stock_price(stock_, headers)
            print(f"{stock_['ticker']} - {stock_['name']} - atualizado")
        return fundos

    def att_prices(self, headers, daily=False):
        if not daily:
            fundos = http_repository.get_objects("stocks", ["investment_type_id"], {"investment_type_id": 15}, headers)
            self.att_prices_generic(headers, fundos, 'fundo', daily)
        fiis = load_fiis_info()
        self.att_prices_generic(headers, fiis, 'fii', daily)
        acoes = load_acoes_info()
        self.att_prices_generic(headers, acoes, 'acao', daily)
        bdrs = load_bdr_info()
        self.att_prices_generic(headers, bdrs, 'bdr', daily)

    def att_prices_generic(self, headers, stocks, type, daily=False):
        for stock in stocks:
            self.att_price_generic(headers, stock, type, daily)

    def att_price_generic(self, headers, stock, type, daily=False):
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
        investment_handler.att_stock_price_new(headers, daily, stock, stock_, type)

    def update_stock(self, headers, stock):
        if stock['investment_type'] == 2:
            self.att_price_generic(headers, stock, 'fii')
        elif stock['investment_type'] == 1:
            self.att_price_generic(headers, stock, 'acao')
        elif stock['investment_type'] == 4:
            self.att_price_generic(headers, stock, 'bdr')
        return stock
