# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import decimal
import json
import threading
import time
from datetime import datetime, timedelta

import psycopg2
import schedule as schedule
from flask import Flask, request
from flask_cors import CORS, cross_origin

from prometheus_flask_exporter import PrometheusMetrics

from service.AttStocks import AttStocks
from service.FiiHandler import FiiHandler
from service.InvestmentHandler import InvestmentHandler
from service.LoadInfo import load_fiis_info
from service.StocksHandler import StocksHandler
from service.Utils import Utils

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

metrics = PrometheusMetrics(app, group_by='endpoint')

conn = psycopg2.connect(
    host="postgres",
    database="postgres",
    user="postgres",
    password="postgres")

fii_handler = FiiHandler()
investment_handler = InvestmentHandler()
att_stocks = AttStocks()
stocks_handler = StocksHandler()
utils = Utils()


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)


@app.route('/att-all', methods=['POST'])
def att_all():  # put application's code here
    att_stocks.att_all()
    return "{}", 200, {'Content-Type': 'application/json'}


@app.route('/att-bdr', methods=['POST'])
def att_bdr():  # put application's code here
    att_stocks.att_bdr()
    return "{}", 200, {'Content-Type': 'application/json'}


@app.route('/att-fiis', methods=['POST'])
def hello_world():  # put application's code here
    fii_handler.att_fiis(load_fiis_info())
    return 'Hello World!'


@app.route('/fiis', methods=['GET'])
@cross_origin()
def get_fiis_list():
    fiis = fii_handler.get_fiis()
    return json.dumps(fiis, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/stocks-br', methods=['GET'])
@cross_origin()
def get_stocks_br():
    stocks = stocks_handler.get_stocks(1)
    return json.dumps(stocks, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/bdrs', methods=['GET'])
@cross_origin()
def get_bdrs():
    stocks = stocks_handler.get_stocks(4)
    return json.dumps(stocks, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/founds', methods=['GET'])
@cross_origin()
def get_founds():
    stocks = stocks_handler.get_founds()
    return json.dumps(stocks, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/investment-movement', methods=['POST'])
@cross_origin()
def add_movement():
    return json.dumps(investment_handler.add_movement(request.get_json())), 200, {'Content-Type': 'application/json'}


@app.route('/investment-movements', methods=['POST'])
@cross_origin()
def add_movements():
    return json.dumps(investment_handler.add_movements(request.get_json())), 200, {'Content-Type': 'application/json'}


@app.route('/investments', methods=['GET'])
@cross_origin()
def get_investments():
    try:
        consolidated = investment_handler.get_stocks_consolidated()
        dumps = json.dumps(consolidated)
        return dumps, 200, {'Content-Type': 'application/json'}
    except Exception as e:
        msg = {'error': str(e)}
        return json.dumps(msg), 500, {'Content-Type': 'application/json'}


def att_expres():
    if utils.work_day() and utils.work_time():
        print("att_express start")
        att_stocks.att_expres()
        print("att_express done")


@app.route('/att-prices', methods=['POST'])
def att_prices():
    att_stocks.att_prices(True)
    return "{}", 200, {'Content-Type': 'application/json'}


schedule.every(15).minutes.do(att_expres)


def schedule_job():
    while True:
        schedule.run_pending()
        time.sleep(1)


t1 = threading.Thread(target=schedule_job, args=())
t1.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
