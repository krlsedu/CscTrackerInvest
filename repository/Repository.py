import psycopg2

from service.Interceptor import Interceptor


class GenericRepository(Interceptor):
    def __init__(self):
        self.conn = psycopg2.connect(
            host="postgres",
            database="postgres",
            user="postgres",
            password="postgres")

    def execute_select(self, select_):
        cursor = self.conn.cursor()
        cursor.execute(select_)
        cursor_ = cursor.fetchall()
        return cursor, cursor_

    def execute_query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()

    def exist_fii(self, fii):
        select_ = f"select * from fiis where ticker='{fii['ticker']}'"
        cursor, cursor_ = self.execute_select(select_)
        exist = cursor_.__len__() > 0
        cursor.close()
        return exist

    def get_fii(self, fii):
        keys = ['last_update', 'desv_dy', 'segment']
        ks = str(keys).replace("[", "").replace("]", "").replace("'", "")
        select_ = f"select {ks} from fiis where ticker='{fii['ticker']}'"
        cursor, cursor_ = self.execute_select(select_)
        for row in cursor_:
            i = 0
            for key in keys:
                fii[key] = row[i]
                i += 1
        cursor.close()
        return fii

    def get_fiis(self):
        keys = ['ticker', 'price', 'dy', 'lastdividend', 'p_vp', 'segment']
        ks = str(keys).replace("[", "").replace("]", "").replace("'", "")
        select_ = f"select " \
                  f"    {ks} " \
                  f"from " \
                  f"    fiis " \
                  f"where " \
                  f"    dy > 0 " \
                  f"    and rank_pvp > 0  " \
                  f"    and rank_dy > 0  " \
                  f"    and rank_desv_dy > 0  " \
                  f"    and liquidezmediadiaria > 500000 " \
                  f"order by " \
                  f"    rank_desv_dy + rank_dy + rank_pvp"
        cursor, cursor_ = self.execute_select(select_)
        fiis = []
        for row in cursor_:
            i = 0
            fii = {}
            for key in keys:
                fii[key] = row[i]
                i += 1
            fii['url_fundamentos'] = f"https://www.fundamentus.com.br/detalhes.php?papel={fii['ticker']}"
            fii['url_statusinvest'] = f"https://statusinvest.com.br/fundos-imobiliarios/{fii['ticker']}"
            fiis.append(fii)
        cursor.close()
        return fiis
