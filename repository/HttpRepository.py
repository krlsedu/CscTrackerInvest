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
            queue = time > (1000 * 60 * 60 * 12) or time < 0 or force
        except Exception as e:
            queue = True
        if queue:
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
                td__text = td[3].find_all("div")[0].text
                __object = float(td__text.replace(".", "").replace(",", "."))
                if last_dy is None:
                    last_dy = __object
                values.append(__object)
                pass
        try:
            f = stdev(values)
            mean1 = mean(values)
            stock["desv_dy"] = f / mean1
        except:
            stock["desv_dy"] = 0

        try:
            stock["segment"] = soup.find_all(text="Segmento ANBIMA")[0].parent.parent.find_all("strong")[0].text
        except:
            stock["segment"] = soup.find_all(text="Segmento de Atuação")[0].parent.parent.find_all("strong")[0].text

        try:
            price_text = soup.find_all(title="Valor atual do ativo")[0].parent.parent.find_all("strong")[0].text
        except:
            price_text = soup.find_all(title="Valor atual")[0].parent.parent.find_all("strong")[0].text
        stock["price"] = float(price_text.replace(".", "").replace(",", "."))

        try:
            dy_txt = soup.find_all(title="Dividend Yield com base nos últimos 12 meses")[0].parent.find_all("strong")[0].text
            stock["dy"] = float(dy_txt.replace(".", "").replace(",", "."))
        except Exception as e:
            stock["dy"] = 0

        try:
            txt = soup.find_all(text="P/L")[0].parent.parent.parent.find_all("strong")[0].text
            stock["pl"] = float(txt.replace(".", "").replace(",", "."))
        except Exception as e:
            stock["pl"] = 0

        try:
            txt = soup.find_all(text="P/VP")[0].parent.parent.parent.find_all("strong")[0].text
            stock["pvp"] = float(txt.replace(".", "").replace(",", "."))
        except Exception as e:
            stock["pvp"] = 0

        try:
            txt = soup.find_all(text="Liquidez média diária")[0].parent.parent.parent.find_all("strong")[0].text
            stock["avg_liquidity"] = float(txt.replace(".", "").replace(",", "."))
        except Exception as e:
            stock["avg_liquidity"] = 0

        try:
            txt = soup.find_all(text="Último rendimento")[0].parent.parent.parent.find_all("strong")[0].text
            stock["last_dividend"] = float(txt.replace(".", "").replace(",", "."))
        except Exception as e:
            if last_dy is not None:
                stock["last_dividend"] = last_dy
            else:
                stock["last_dividend"] = 0
            pass

        return stock
