from flask import request

from repository.Repository import GenericRepository
from service.Interceptor import Interceptor

generic_repository = GenericRepository()


class StocksHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def get_stocks_basic(self):
        select_ = f"select " \
                  f"    ticker, ticker || ' - ' || name as name " \
                  f"from " \
                  f"    stocks "
        return generic_repository.get_objects_from_sql(select_)


    def get_stocks(self, type_, args=None):
        if args is None:
            args = request.args
        liquidez = args.get('avg_liquidity')
        if liquidez is None:
            liquidez = 500000
        else:
            liquidez = float(liquidez)
        keys = ['ticker', 'price', 'dy', 'last_dividend', 'pvp', 'segment', 'pl', 'name', 'investment_type_id',
                'url_infos', 'ev_ebit']
        ks = str(keys).replace("[", "").replace("]", "").replace("'", "")
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
                  f"    and (ev_ebit > 0 or investment_type_id = 4)" \
                  f"    and avg_liquidity > {liquidez} " \
                  f"order by " \
                  f"    ev_ebit, rank_dy + rank_desv_dy + rank_pl + rank_pvp"
        cursor, cursor_ = generic_repository.execute_select(select_)
        stocks = []
        rank = 1
        tikers_prefix = []
        for row in cursor_:
            i = 0
            stock = {}
            for key in keys:
                stock[key] = row[i]
                i += 1
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
        cursor.close()
        return stocks

    def get_founds(self, id):
        keys = ['ticker', 'price', 'dy', 'last_dividend', 'pvp', 'segment', 'pl', 'name', 'investment_type_id',
                'url_infos']
        ks = str(keys).replace("[", "").replace("]", "").replace("'", "")
        select_ = f"select " \
                  f"    {ks} " \
                  f"from " \
                  f"    stocks " \
                  f"where " \
                  f"    investment_type_id = {id}  " \
                  f"order by " \
                  f"    name"
        cursor, cursor_ = generic_repository.execute_select(select_)
        stocks = []
        rank = 1
        for row in cursor_:
            i = 0
            stock = {}
            for key in keys:
                stock[key] = row[i]
                i += 1
            stock['url_fundamentos'] = f"https://www.fundamentus.com.br/detalhes.php?papel={stock['ticker']}"
            stock['url_statusinvest'] = f"https://statusinvest.com.br{stock['url_infos']}"
            stock['rank'] = rank
            rank += 1
            stocks.append(stock)
        cursor.close()
        return stocks

    def get_price(self, investiment_id, date):
        select_ = f"select * from stocks_prices where " \
                  f"date_value <= '{date}' " \
                  f"and investment_id = {investiment_id} " \
                  f"order by date_value desc limit 1"
        cursor, cursor_ = generic_repository.execute_select(select_)
        col_names = cursor.description
        obj = {}
        for row in cursor_:
            i = 0
            for col_name in col_names:
                obj[col_name[0]] = row[i]
                i += 1
            cursor.close()
            return obj

        cursor.close()
