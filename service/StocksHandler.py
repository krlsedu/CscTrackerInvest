from flask import request

from repository.HttpRepository import HttpRepository
from service.Interceptor import Interceptor

http_repository = HttpRepository()


class StocksHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def get_stocks_basic(self, headers=None):
        select_ = f"select " \
                  f"    ticker, ticker || ' - ' || name as name " \
                  f"from " \
                  f"    stocks "
        return http_repository.execute_select(select_, headers)

    def get_stocks(self, type_, headers=None, args=None):
        if args is None:
            args = request.args
        liquidez = args.get('avg_liquidity')
        if liquidez is None:
            liquidez = 250000
        else:
            liquidez = float(liquidez)
        keys = ['ticker', 'price', 'dy', 'last_dividend', 'pvp', 'segment', 'pl', 'name', 'investment_type_id',
                'url_infos', 'ev_ebit']
        ks = str(keys).replace("[", "").replace("]", "").replace("'", "")
        if type_ == 1:
            select_ = f"select " \
                      f"    {ks} " \
                      f"from " \
                      f"    stocks " \
                      f"where " \
                      f"    investment_type_id = {type_}  " \
                      f"    and rank_pvp > 0  " \
                      f"    and rank_pl > 0  " \
                      f"    and pl >= 0  " \
                      f"    and pvp >= 0  " \
                      f"    and (ev_ebit > 0)" \
                      f"    and avg_liquidity > {liquidez} " \
                      f"order by " \
                      f"    ev_ebit, rank_dy + rank_desv_dy + rank_pl + rank_pvp"
        else:
            select_ = f"select " \
                      f"    {ks} " \
                      f"from " \
                      f"    stocks " \
                      f"where " \
                      f"    investment_type_id = 4  " \
                      f"    and rank_pvp > 0  " \
                      f"    and rank_pl > 0  " \
                      f"    and pl >= 0  " \
                      f"    and pvp >= 0  " \
                      f"    and ev is not null " \
                      f"    and ev > 0 " \
                      f"    and ebitda is not null " \
                      f"    and ebitda > 0 " \
                      f"    and dy > 0 " \
                      f"    and avg_liquidity > 100000 " \
                      f"order by " \
                      f"    ev / ebitda, rank_dy + rank_desv_dy + rank_pl + rank_pvp"

        objects = http_repository.execute_select(select_, headers)
        stocks = []
        rank = 1
        tikers_prefix = []
        for stock in objects:
            stock['url_fundamentos'] = f"https://www.fundamentus.com.br/detalhes.php?papel={stock['ticker']}"
            stock['url_statusinvest'] = f"https://statusinvest.com.br{stock['url_infos']}"
            tiker_prefix = ''.join([i for i in stock['ticker'] if not i.isdigit()])
            if tiker_prefix not in tikers_prefix:
                stock['rank'] = rank
                rank += 1
                tikers_prefix.append(tiker_prefix)
            else:
                stock['rank'] = rank + 10000
            stocks.append(stock)
        return stocks

    def get_founds(self, id_, headers=None):
        keys = ['ticker', 'price', 'dy', 'last_dividend', 'pvp', 'segment', 'pl', 'name', 'investment_type_id',
                'url_infos']
        ks = str(keys).replace("[", "").replace("]", "").replace("'", "")
        select_ = f"select " \
                  f"    {ks} " \
                  f"from " \
                  f"    stocks " \
                  f"where " \
                  f"    investment_type_id = {id_}  " \
                  f"order by " \
                  f"    name"
        objects = http_repository.execute_select(select_, headers)
        stocks = []
        rank = 1
        for stock in objects:
            stock['url_fundamentos'] = f"https://www.fundamentus.com.br/detalhes.php?papel={stock['ticker']}"
            stock['url_statusinvest'] = f"https://statusinvest.com.br{stock['url_infos']}"
            stock['rank'] = rank
            rank += 1
            stocks.append(stock)
        return stocks

    def get_price(self, investiment_id, date, headers=None):
        select_ = f"select * from stocks_prices where " \
                  f"date_value <= '{date}' " \
                  f"and investment_id = {investiment_id} " \
                  f"order by date_value desc limit 1"
        response = http_repository.execute_select(select_, headers)
        return response[0]
