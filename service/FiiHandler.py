import json
from datetime import datetime
from statistics import stdev, mean

import pandas as pd
from bs4 import BeautifulSoup
import requests
from flask import request

from repository.HttpRepository import HttpRepository
from repository.Repository import GenericRepository
from service.Interceptor import Interceptor
from service.LoadInfo import load_fiis_info

generic_repository = GenericRepository()
http_repository = HttpRepository()

class FiiHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def att_fiis(self, fiis):
        for fii in fiis:
            ticker = fii['ticker']
            url = requests.get(
                f"https://statusinvest.com.br/fundos-imobiliarios/{ticker}")
            nav = BeautifulSoup(url.text, "html5lib")

            self.get_all_values_fiis(nav, fii)
            print(f"Atualizando o fundo: {ticker}")

        fiis = self.calc_ranks(fiis)
        for fii in fiis:
            try:
                self.write_fii(fii)
            except Exception as e:
                print(e)
                print(fii)
                pass

    def get_fiis(self, args=None):
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
        fiis = generic_repository.get_fiis(liquidez)
        rank = 1
        for fii in fiis:
            try:
                fii['rank'] = rank
                rank += 1
                fii['price'] = values_fiis[fii['ticker']]
                self.att_price(fii)
            except Exception as e:
                pass
        return fiis

    def calc_ranks(self, fiis):
        df = pd.DataFrame.from_dict(fiis)
        df['rank_dy'] = df['desv_dy'].rank(ascending=False)
        df['rank_pvp'] = df['p_vp'].rank(ascending=True)
        df['rank_desv_dy'] = df['desv_dy'].rank(ascending=True)

        return json.loads(df.to_json(orient="records"))

    def get_all_values_fiis(self, soup, fii):
        ticker = fii['ticker']
        try:
            fii = generic_repository.get_fii(fii)
            try:
                time = datetime.now().timestamp() * 1000 - fii['last_update'].timestamp() * 1000
                queue = time > (1000 * 60 * 60 * 12)
            except Exception as e:
                queue = True
            if queue:
                self.get_values_money_fiis(soup, fii)
        except Exception as e:
            print(e)
            print("Erro ao escrever dados do fii: " + ticker)

    def get_fii(self, fii):
        fii = generic_repository.get_object("fiis", ["ticker"], fii)
        try:
            time = datetime.now().timestamp() * 1000 - fii['last_update'].timestamp() * 1000
            queue = time > (1000 * 60 * 15)
        except Exception as e:
            queue = True
        if queue:
            try:
                fii = http_repository.get_values_by_ticker(fii)
                generic_repository.update("fiis", ["ticker"], fii)
            except Exception as e:
                pass
        return fii

    def att_price(self, fii):
        generic_repository.execute_query(
            f"update fiis "
            f"SET last_update = now(),"
            f"  price = {fii['price']} "
            f"where ticker='{fii['ticker']}'"
        )

    def write_fii(self, fii):
        for key, value in fii.items():
            if value is None:
                fii[key] = 0

        if generic_repository.exist_fii(fii):
            generic_repository.execute_query(
                f"update fiis "
                f"SET companyName = '{fii['companyName']}',"
                f"last_update = now(),"
                f"ticker = '{fii['ticker']}',"
                f"gestao = '{fii['gestao']}',"
                f"segment = '{fii['segment']}',"
                f"price = {fii['price']},"
                f"dy = {fii['dy']},"
                f"p_vp = {fii['p_vp']},"
                f"valorpatrimonialcota = {fii['valorpatrimonialcota']},"
                f"liquidezmediadiaria = {fii['liquidezmediadiaria']},"
                f"percentualcaixa = {fii['percentualcaixa']},"
                f"dividend_cagr = {fii['dividend_cagr']},"
                f"numerocotistas = {fii['numerocotistas']},"
                f"numerocotas = {fii['numerocotas']},"
                f"patrimonio = {fii['patrimonio']},"
                f"lastdividend = {fii['lastdividend']},"
                f"desv_dy = {fii['desv_dy']},"
                f"rank_desv_dy = {fii['rank_desv_dy']},"
                f"rank_dy = {fii['rank_dy']},"
                f"rank_pvp = {fii['rank_pvp']}"
                f" where ticker='{fii['ticker']}'"
            )
            # print("Ação atualizada")
        else:
            generic_repository.execute_query(
                f"insert into fiis("
                f"companyName, "
                f"ticker, "
                f"gestao, "
                f"segment, "
                f"price, "
                f"dy, "
                f"p_vp, "
                f"valorpatrimonialcota, "
                f"liquidezmediadiaria, "
                f"percentualcaixa, "
                f"dividend_cagr, "
                f"numerocotistas, "
                f"numerocotas, "
                f"patrimonio, "
                f"lastdividend, "
                f"desv_dy, "
                f"rank_desv_dy, "
                f"rank_dy, "
                f"rank_pvp) values("
                f"'{fii['companyName']}',"
                f"'{fii['ticker']}',"
                f"'{fii['gestao']}',"
                f"'{fii['segment']}',"
                f"{fii['price']},"
                f"{fii['dy']},"
                f"{fii['p_vp']},"
                f"{fii['valorpatrimonialcota']},"
                f"{fii['liquidezmediadiaria']},"
                f"{fii['percentualcaixa']},"
                f"{fii['dividend_cagr']},"
                f"{fii['numerocotistas']},"
                f"{fii['numerocotas']},"
                f"{fii['patrimonio']},"
                f"{fii['lastdividend']},"
                f"{fii['desv_dy']},"
                f"{fii['rank_desv_dy']},"
                f"{fii['rank_dy']},"
                f"{fii['rank_pvp']}"
                f")"
            )

    def get_values_money_fiis(self, soup, fii):
        values = []
        find_all = soup.find_all("table")[0].find_all("tr")
        for tr in find_all:
            td = tr.find_all("td")
            try:
                td__text = td[3].text
                values.append(float(td__text.replace(".", "").replace(",", ".")))
            except IndexError:
                pass
            except ValueError:
                td__text = td[3].find_all("div")[0].text
                values.append(float(td__text.replace(".", "").replace(",", ".")))
                pass
        try:
            f = stdev(values)
            mean1 = mean(values)
            fii["desv_dy"] = f / mean1
        except:
            fii["desv_dy"] = 0
        fii["segment"] = soup.find_all(text="Segmento ANBIMA")[0].parent.parent.find_all("strong")[0].text
        price_text = soup.find_all(title="Valor atual do ativo")[0].parent.parent.find_all("strong")[0].text
        fii["price"] = float(price_text.replace(".", "").replace(",", "."))

        return fii
