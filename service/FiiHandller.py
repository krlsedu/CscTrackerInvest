import json
from datetime import datetime
from statistics import stdev, mean

import pandas as pd
from bs4 import BeautifulSoup
import requests

from repository.Repository import GenericRepository

generic_repository = GenericRepository()


def attFiis(fiis):
    for fii in fiis:
        ticker = fii['ticker']
        url = requests.get(
            f"https://statusinvest.com.br/fundos-imobiliarios/{ticker}")
        nav = BeautifulSoup(url.text, "html5lib")

        getAllValuesFiis(nav, fii)
        print(f"Atualizando o fundo: {ticker}")

    fiis = calcRanks(fiis)
    for fii in fiis:
        try:
            write_fii(fii)
        except Exception as e:
            print(e)
            print(fii)
            pass


def calcRanks(fiis):
    df = pd.DataFrame.from_dict(fiis)
    df['rank_dy'] = df['desv_dy'].rank(ascending=False)
    df['rank_pvp'] = df['p_vp'].rank(ascending=True)
    df['rank_desv_dy'] = df['desv_dy'].rank(ascending=True)

    js = json.loads(df.to_json(orient="records"))
    print(js)
    return js


def getAllValuesFiis(soup, fii):
    ticker = fii['ticker']
    try:
        fii = generic_repository.get_fii(fii)
        try:
            time = datetime.now().timestamp() * 1000 - fii['last_update'].timestamp() * 1000
            queue = time > (1000 * 60 * 60 * 24)
        except Exception as e:
            queue = True
        if queue:
            getValuesMoneyFiis(soup, fii)
    except Exception as e:
        print(e)
        print("Erro ao escrever dados do fii: " + ticker)


def write_fii(fii):
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


def getValuesMoneyFiis(soup, fii):
    values = []
    find_all = soup.find_all("table")[0].find_all("tr")
    for tr in find_all:
        td = tr.find_all("td")
        try:
            td__text = td[3].text
            values.append(float(td__text.replace(".", "").replace(",", ".")))
        except IndexError:
            pass
    f = stdev(values)
    mean1 = mean(values)
    fii["desv_dy"] = f / mean1

    fii["segment"] = soup.find_all(text="Segmento ANBIMA")[0].parent.parent.find_all("strong")[0].text

    return fii
