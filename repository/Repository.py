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
