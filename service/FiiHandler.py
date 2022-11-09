from flask import request

from repository.HttpRepository import HttpRepository
from service.Interceptor import Interceptor
from service.LoadInfo import load_fiis_info

http_repository = HttpRepository()

headers_sti = {
    'Cookie': '_adasys=5aeb049b-bdfc-4984-9901-bf3539f577b1',
    'User-Agent': 'PostmanRuntime/7.26.8'
}


class FiiHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def get_fiis(self, headers, args=None):
        load_fiis = load_fiis_info()
        values_fiis = {}
        for fii in load_fiis:
            values_fiis[fii['ticker']] = fii['price']

        if args is None:
            args = request.args
        liquidez = args.get('metric')
        if liquidez is None:
            liquidez = 500000
        else:
            liquidez = float(liquidez)

        keys = ['ticker', 'price', 'dy', 'last_dividend', 'pvp', 'segment', 'name', 'investment_type_id']
        ks = str(keys).replace("[", "").replace("]", "").replace("'", "")
        select_ = f"select " \
                  f"    {ks} " \
                  f"from " \
                  f"    stocks " \
                  f"where " \
                  f"    dy > 0 " \
                  f"    and investment_type_id = 2  " \
                  f"    and rank_pvp > 0  " \
                  f"    and rank_dy > 0  " \
                  f"    and rank_desv_dy > 0  " \
                  f"    and avg_liquidity > {liquidez} " \
                  f"order by " \
                  f"    rank_desv_dy + rank_dy + rank_pvp"
        fiis = http_repository.execute_select(select_, headers)
        rank = 1
        for fii in fiis:
            try:
                fii['rank'] = rank
                rank += 1
                fii['price'] = values_fiis[fii['ticker']]
            except Exception as e:
                pass
        return fiis
