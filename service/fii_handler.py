import logging

from csctracker_py_core.repository.http_repository import HttpRepository
from csctracker_py_core.repository.remote_repository import RemoteRepository

from service.load_info import LoadInfo


class FiiHandler:
    def __init__(self,
                 load_info: LoadInfo,
                 remote_repository: RemoteRepository,
                 http_repository: HttpRepository):
        self.logger = logging.getLogger()
        self.load_info = load_info
        self.remote_repository = remote_repository
        self.http_repository = http_repository

    def get_fiis(self, headers, args=None):
        load_fiis = self.load_info.load_fiis_info(headers)
        values_fiis = {}
        for fii in load_fiis:
            values_fiis[fii['ticker']] = fii['price']

        if args is None:
            args = self.http_repository.get_args()
        liquidez = args.get('metric')
        if liquidez is None:
            liquidez = 250000
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
        fiis = self.remote_repository.execute_select(select_, headers)
        rank = 1
        for fii in fiis:
            try:
                fii['rank'] = rank
                rank += 1
                fii['price'] = values_fiis[fii['ticker']]
            except Exception as e:
                pass
        return fiis
