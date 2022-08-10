import datetime

import psycopg2
from flask import request

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
        cursor.close()
        self.conn.commit()

    def exist_fii(self, fii):
        select_ = f"select * from fiis where ticker='{fii['ticker']}'"
        return self.exist(select_)

    def get_fiis(self, liquidez):
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
                  f"    and liquidezmediadiaria > {liquidez} " \
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

    def exist(self, select_):
        cursor, cursor_ = self.execute_select(select_)
        exist = cursor_.__len__() > 0
        cursor.close()
        return exist

    def exist_by_key(self, table, key=[], data={}):
        select_ = f"select * from {table} where "
        select_ = self.wheres(data, key, select_)
        return self.exist(select_)

    def wheres(self, data, key=[], select_=""):
        for i in range(key.__len__()):
            key_i_ = key[i]
            i_ = data[key_i_]
            tp_ = type(i_)
            if tp_ == str:
                select_ += f"{key_i_}='{i_}'"
            else:
                select_ += f"{key_i_}={i_}"
            if i < key.__len__() - 1:
                select_ += " and "
        return select_

    def insert(self, table, data):
        keys_ = data.keys()
        keys = []
        values = []
        for key in keys_:
            keys.append(key)
            values.append(data[key])
        ks = str(keys).replace("[", "").replace("]", "").replace("'", "")
        vs = str(values).replace("[", "").replace("]", "")
        insert_ = f"insert into {table} ({ks}) values ({vs})"
        self.execute_query(insert_)

    def update(self, table, col_pk=[], data={}):
        keys_ = data.keys()
        keys = []
        values = []
        data['last_update'] = datetime.datetime.now()
        for key in keys_:
            keys.append(key)
            values.append(data[key])
        update_ = f"update {table} set "
        for i in range(keys.__len__()):
            i_ = values[i]
            tp_ = type(i_)
            if tp_ == str or tp_ == datetime.datetime:
                update_ += f"{keys[i]}='{i_}'"
            else:
                update_ += f"{keys[i]}={i_}"
            if i < keys.__len__() - 1:
                update_ += ", "
        update_ += f" where "
        update_ = self.wheres(data, col_pk, update_)
        self.execute_query(update_)

    def get_object(self, table, keys=[], data={}, object_=None):
        if object_ is None:
            ks = "*"
        else:
            ks = object_.__dict__.keys()
            ks = str(ks).replace("[", "").replace("]", "")
        select_ = f"select {ks} from {table} where "
        select_ = self.wheres(data, keys, select_)
        cursor, cursor_ = self.execute_select(select_)
        col_names = cursor.description
        obj = {}
        for row in cursor_:
            i = 0
            for col_name in col_names:
                obj[col_name[0]] = row[i]
                i += 1
            cursor.close()
            return obj

    def get_objects(self, table, keys=[], data={}, object_=None):
        if object_ is None:
            ks = "*"
        else:
            ks = object_.__dict__.keys()
            ks = str(ks).replace("[", "").replace("]", "")
        select_ = f"select {ks} from {table} where "
        select_ = self.wheres(data, keys, select_)
        cursor, cursor_ = self.execute_select(select_)
        col_names = cursor.description
        objects = []
        for row in cursor_:
            obj = {}
            i = 0
            for col_name in col_names:
                obj[col_name[0]] = row[i]
                i += 1
            objects.append(obj)
        cursor.close()
        return objects

    def add_user_id(self, data):
        user = self.get_user()
        data['user_id'] = user['id']
        return data

    def get_user(self):
        user_name = request.headers.get('userName')
        user = self.get_object('users', ['email'], {'email': user_name})
        return user
