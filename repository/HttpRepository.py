import json
from datetime import datetime
from statistics import stdev, mean

import requests
from bs4 import BeautifulSoup

from repository.Repository import GenericRepository
from service.Interceptor import Interceptor

generic_repository = GenericRepository()


class HttpRepository(Interceptor):
    def __init__(self):
        super().__init__()

    def get_stock_type(self, ticker):
        response = requests.get('https://statusinvest.com.br/home/mainsearchquery', params={"q": ticker})
        return response.json()

    def get_firt_stock_type(self, ticker):
        return self.get_stock_type(ticker)[0]

    def get_page_text(self, ticker):
        stock = generic_repository.get_object("stocks", ["ticker"], {"ticker": ticker})
        page = requests.get(f"https://statusinvest.com.br{stock['url_infos']}")
        return page.text

    def get_values_by_ticker(self, stock, force=False):
        try:
            time = datetime.now().timestamp() * 1000 - stock['last_update'].timestamp() * 1000
            queue = time > (1000 * 60 * 15) or time < 0
        except Exception as e:
            queue = True
        if queue or force:
            try:
                text = self.get_page_text(stock['ticker'])
                soup = BeautifulSoup(text, "html5lib")
                self.get_values(soup, stock)
                generic_repository.update("stocks", ["ticker"], stock)
            except Exception as e:
                print(e)
                pass
        investment_type = generic_repository.get_object("investment_types", ["id"], {"id": stock['investment_type_id']})
        return stock, investment_type

    def get_values(self, soup, stock):
        values = []
        find_all = soup.find_all("table")[0].find_all("tr")
        last_dy = None
        for tr in find_all:
            td = tr.find_all("td")
            try:
                td__text = td[3].text
                float1 = float(td__text.replace(".", "").replace(",", "."))
                if last_dy is None:
                    last_dy = float1
                values.append(float1)
            except IndexError:
                pass
            except ValueError:
                try:
                    td__text = td[3].find_all("div")[0].text
                    __object = float(td__text.replace(".", "").replace(",", "."))
                    if last_dy is None:
                        last_dy = __object
                    values.append(__object)
                except:
                    pass

        try:
            f = stdev(values)
            mean1 = mean(values)
            stock["desv_dy"] = f / mean1
        except:
            stock["desv_dy"] = 0

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
            dy_txt = self.find_value(soup, "Dividend Yield com base nos últimos 12 meses", "title", 1, 0)
            stock["dy"] = float(dy_txt.replace(".", "").replace(",", "."))
        except Exception as e:
            stock["dy"] = 0

        try:
            txt = self.find_value(soup, "P/L", "text", 3, 0)
            stock["pl"] = float(txt.replace(".", "").replace(",", "."))
        except Exception as e:
            stock["pl"] = 0

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

        try:
            txt = self.find_value(soup, "Último rendimento", "text", 3, 0)
            stock["last_dividend"] = float(txt.replace(".", "").replace(",", "."))
        except Exception as e:
            if last_dy is not None:
                stock["last_dividend"] = last_dy
            else:
                stock["last_dividend"] = 0
            pass
        #
        # try:
        #     txt = self.find_value(soup, "earning-section", "id", 0, 0, "input", "value")
        #     txt = json.loads(txt)
        # #     company-relateds
        # except Exception as e:
        #     pass

        return stock

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

    def get_prices(self, ticker, type, daily=False):
        if daily:
            response = requests.get(f'https://statusinvest.com.br/{type}/tickerprice?type=-1&currences%5B%5D=1',
                                params={"ticker": ticker})
        else:
            response = requests.get(f'https://statusinvest.com.br/{type}/tickerprice?type=4&currences%5B%5D=1',
                                params={"ticker": ticker})
        return response.json()

    def get_prices_fundos(self, ticker):
        response = requests.get(f'https://statusinvest.com.br/fundoinvestimento/profitabilitymainresult?'
                                f'nome_clean={ticker}'
                                f'&time=6')
        return response.json()
