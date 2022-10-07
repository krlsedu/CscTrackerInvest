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
from service.RequestHandler import RequestHandler
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
request_handler = RequestHandler()


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)


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
    stocks = stocks_handler.get_founds(15)
    return json.dumps(stocks, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/investment-movement', methods=['POST'])
@cross_origin()
def add_movement():
    dumps = json.dumps(investment_handler.add_movement(request.get_json()))
    get_investments()
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investment-movements', methods=['POST'])
@cross_origin()
def add_movements():
    dumps = json.dumps(investment_handler.add_movements(request.get_json()))
    get_investments()
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investments', methods=['GET'])
@cross_origin()
def get_investments():
    try:
        headers = request.headers
        args = request.args
        threading.Thread(target=get_investments_tr, args=(args, headers,)).start()
        message = {
            'text': 'Investments update requested',
            'status': 'ok'
        }
        return message, {'Content-Type': 'application/json'}
    except Exception as e:
        message = {
            'text': 'Investments update requested',
            'status': 'error',
            'error': str(e)
        }
        print(e)
        return json.dumps(message), 500, {'Content-Type': 'application/json'}


def get_investments_tr(args, headers):
    try:
        request_handler.inform_to_client("Investments refresh requested", "investments", headers)
        consolidated = investment_handler.buy_sell_indication(args, headers)
        consolidated = json.dumps(consolidated, cls=Encoder, ensure_ascii=False)
        request_handler.inform_to_client(consolidated, "investments", headers)
        return consolidated, 200, {'Content-Type': 'application/json'}
    except Exception as e:
        msg = {'error': str(e)}
        print(e)
        return json.dumps(msg), 500, {'Content-Type': 'application/json'}


@app.route('/att-prices', methods=['POST'])
def att_prices():
    if utils.work_day():
        print('att_prices requested')
        threading.Thread(target=att_prices_thr()).start()
    return "{}", 200, {'Content-Type': 'application/json'}


def att_prices_thr():
    att_stocks.att_prices()
    att_stocks.att_prices(True)


@app.route('/att-express', methods=['POST'])
def att_express():
    if utils.work_day() and utils.work_time():
        print("att_express requested")
        threading.Thread(target=att_stocks.att_expres).start()
    return "{}", 200, {'Content-Type': 'application/json'}


@app.route('/att-bdr', methods=['POST'])
def att_bdr():
    if utils.work_day():
        print("att_bdr requested")
        threading.Thread(target=att_stocks.att_bdr()).start()

    return "{}", 200, {'Content-Type': 'application/json'}


@app.route('/att-full', methods=['POST'])
def att_full():  # put application's code here
    print("att_full requested")
    threading.Thread(target=att_stocks.att_full()).start()
    return "{}", 200, {'Content-Type': 'application/json'}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
