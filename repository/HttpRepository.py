from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from service.Interceptor import Interceptor

headers_sti = {
    'Cookie': '_adasys=5aeb049b-bdfc-4984-9901-bf3539f577b1',
    'User-Agent': 'PostmanRuntime/7.26.8'
}

url_repository = 'http://bff:8080/repository/'


class HttpRepository(Interceptor):
    def __init__(self):
        super().__init__()

    def insert(self, table, data, headers=None):
        try:
            response = requests.post(url_repository + table, headers=headers, json=data)
            if response.status_code < 200 or response.status_code > 299:
                raise Exception(f'Error inserting data: {response.text}')
        except Exception as e:
            raise e

    def update(self, table, keys=[], data={}, headers=None):
        params = {}
        for key in keys:
            params[key] = data[key]
        try:
            response = requests.post(url_repository + table, headers=headers, json=data, params=params)
            if response.status_code < 200 or response.status_code > 299:
                raise Exception(f'Error updating data: {response.text}')
        except Exception as e:
            raise e

    def delete_all(self, table, headers=None):
        self.delete(table, [], {}, headers)

    def delete(self, table, keys=[], data={}, headers=None):
        params = {}
        for key in keys:
            params[key] = data[key]
        try:
            response = requests.post(url_repository + "delete/" + table, headers=headers, json=data, params=params)
            if response.status_code < 200 or response.status_code > 299:
                raise Exception(f'Error deleting data: {response.text}')
        except Exception as e:
            raise e

    def get_stock_type(self, ticker, headers=None):
        response = requests.get('https://statusinvest.com.br/home/mainsearchquery', params={"q": ticker},
                                headers=headers_sti)
        return response.json()

    def add_user_id(self, data, headers=None):
        user = self.get_user(headers)
        data['user_id'] = user['id']
        return data

    def get_user(self, headers=None):
        user_name = headers.get('userName')
        try:
            user = self.get_object('users', ['email'], {'email': user_name}, headers)
            return user
        except Exception as e:
            raise e

    def get_object(self, table, keys=[], data={}, headers=None):
        params = {}
        for key in keys:
            params[key] = data[key]
        try:
            response = requests.get(url_repository + 'single/' + table, params=params, headers=headers)
            if response.status_code < 200 or response.status_code > 299:
                raise Exception(f'Error getting data: {response.text}')
            return response.json()
        except Exception as e:
            raise e

    def get_all_objects(self, table, headers=None):
        return self.get_objects(table, [], {}, headers)

    def get_objects(self, table, keys=[], data={}, headers=None):
        params = {}
        for key in keys:
            params[key] = data[key]
        try:
            response = requests.get(url_repository + table, params=params, headers=headers)
            if response.status_code < 200 or response.status_code > 299:
                raise Exception(f'Error getting data: {response.text}')
            return response.json()
        except Exception as e:
            raise e

    def execute_select(self, select, headers=None):
        command = {
            'command': select
        }
        try:
            response = requests.post(url_repository + "command/select", headers=headers, json=command)
            if response.status_code < 200 or response.status_code > 299:
                raise Exception(f'Error getting data: {response.text}')
            return response.json()
        except Exception as e:
            raise e

    def exist_by_key(self, table, key=[], data={}, headers=None):
        try:
            objects = self.get_objects(table, key, data, headers)
            return objects.__len__() > 0
        except Exception as e:
            return False

    def get_filters(self, args=None, headers=None):
        filters = []
        values = {}
        for key in args:
            filters.append(key)
            values[key] = args[key]
        return filters, values

    def get_firt_stock_type(self, ticker, headers=None):
        return self.get_stock_type(ticker, headers)[0]

    def get_page_text(self, ticker, headers=None):
        stock = self.get_object("stocks", ["ticker"], {"ticker": ticker}, headers)
        page = requests.get(f"https://statusinvest.com.br{stock['url_infos']}", headers=headers_sti)
        return page.text

    def get_page_text_by_url(self, url, headers=None):
        page = requests.get(f"{url}", headers=headers_sti)
        return page.text

    def get_values_by_ticker(self, stock, force=False, headers=None):
        try:
            last_update = datetime.strptime(stock['last_update'], '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)
            astimezone = datetime.now().astimezone(timezone.utc)
            time = astimezone.timestamp() * 1000 - last_update.timestamp() * 1000
            queue = time > (1000 * 60 * 15) or time < 0
        except Exception as e:
            queue = True
        if queue or force:
            try:
                text = self.get_page_text(stock['ticker'], headers)
                soup = BeautifulSoup(text, "html5lib")
                self.get_values(soup, stock)
                if stock["investment_type_id"] <= 4:
                    stock['pvp'] = self.get_info(stock, "vp", headers)

                requests.post(url_repository + 'stocks', headers=headers,
                              params={"ticker": stock['ticker']}, json=stock)
            except Exception as e:
                print(e)
                pass
        investment_type = requests.get(url_repository + 'single/investment_types',
                                       params={"id": stock['investment_type_id']}, headers=headers).json()
        return stock, investment_type

    def get_soup(self, url, headers=None):
        text = self.get_page_text_by_url(url, headers)
        soup = BeautifulSoup(text, "html5lib")
        return soup

    def get_values(self, soup, stock):
        try:
            stock["segment"] = self.find_value(soup, "Segmento", "text", 2, 0)
        except:
            try:
                stock["segment"] = self.find_value(soup, "Segmento de Atuação", "text", 2, 0)
            except:
                try:
                    stock["segment"] = self.find_value(soup, "\nClasse anbima\n", "text", 2, 0)
                except:
                    pass

        try:
            price_text = self.find_value(soup, "Valor atual do ativo", "title", 2, 0)
        except:
            try:
                price_text = self.find_value(soup, "Valor atual", "title", 2, 0)
            except:
                price_text = self.find_value(soup, "Preço da cota", "title", 2, 0)

        stock["price"] = float(price_text.replace(".", "").replace(",", "."))

        try:
            txt = self.find_value(soup, "P/L", "text", 3, 0)
            stock["pl"] = float(txt.replace(".", "").replace(",", "."))
        except Exception as e:
            stock["pl"] = 0

        if stock["investment_type_id"] > 4:
            try:
                txt = self.find_value(soup, "P/VP", "text", 3, 0)
                stock["pvp"] = float(txt.replace(".", "").replace(",", "."))
            except Exception as e:
                stock["pvp"] = 0

        try:
            txt = self.find_value(soup, "Liquidez média diária", "text", 3, 0)
            stock["avg_liquidity"] = float(txt.replace(".", "").replace(",", "."))
        except Exception as e:
            stock["avg_liquidity"] = 0

        # try:
        #     txt = self.find_value(soup, "earning-section", "id", 0, 0, "input", "value")
        #     txt = json.loads(txt)
        # #     company-relateds
        # except Exception as e:
        #     pass

        return stock

    def get_info(self, stock_, atributo, tag="sapn", headers=None, is_number=True):
        url_ = 'https://investidor10.com.br/fiis/'
        if stock_['investment_type_id'] == 1:
            url_ = 'https://investidor10.com.br/acoes/'
        elif stock_['investment_type_id'] == 4:
            url_ = 'https://investidor10.com.br/bdrs/'
        soup_ = self.get_soup(url_ + stock_['ticker'], headers)
        value_ = self.find_value(soup_, '_card ' + atributo, "class")[0]
        value_ = self.find_value(value_, '_card-body', "class")[0].find_all(tag)[0].text
        if is_number:
            value_ = float(value_.replace(".", "").replace(",", "."))
        return value_

    def find_value(self, soup, text, type, parents=0, children=None, child_Type="strong", value="text"):
        if type == "text":
            obj = soup.find_all(text=f"{text}")
        elif type == "class":
            obj = soup.find_all(class_=f"{text}")
        elif type == "title":
            obj = soup.find_all(title=f"{text}")
        elif type == "id":
            obj = soup.find_all(id=f"{text}")
        else:
            obj = soup.find_all(tag=f"{text}")

        if children is None:
            return obj
        else:
            obj = obj[children]

        for i in range(parents):
            obj = obj.parent
        if value == "text":
            strong__text = obj.find_all(child_Type)[0].text
        else:
            strong__text = obj.find_all(child_Type)[0][value]
        return strong__text

    def get_prices(self, ticker, type, daily=False, price_type="4"):
        if daily:
            response = requests.get(f'https://statusinvest.com.br/{type}/tickerprice?type=-1&currences%5B%5D=1',
                                    params={"ticker": ticker}, headers=headers_sti)
        else:
            response = requests.get(
                f'https://statusinvest.com.br/{type}/tickerprice?type={price_type}&currences%5B%5D=1',
                params={"ticker": ticker}, headers=headers_sti)
        return response.json()

    def get_prices_fundos(self, ticker, month=False):
        if month:
            response = requests.get(f'https://statusinvest.com.br/fundoinvestimento/profitabilitymainresult?'
                                    f'nome_clean={ticker}'
                                    f'&time=1', headers=headers_sti)
        else:
            response = requests.get(f'https://statusinvest.com.br/fundoinvestimento/profitabilitymainresult?'
                                    f'nome_clean={ticker}'
                                    f'&time=6', headers=headers_sti)
        return response.json()
