import decimal
import json
import threading
import time

from flask import Flask, request
from flask_cors import CORS, cross_origin
from prometheus_flask_exporter import PrometheusMetrics

from service.AttStocks import AttStocks
from service.DividendHandler import DividendHandler
from service.FiiHandler import FiiHandler
from service.InvestmentHandler import InvestmentHandler
from service.LoadBalancerRegister import LoadBalancerRegister
from service.RequestHandler import RequestHandler
from service.StocksHandler import StocksHandler
from service.Utils import Utils

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

metrics = PrometheusMetrics(app, group_by='endpoint')

fii_handler = FiiHandler()
investment_handler = InvestmentHandler()
att_stocks = AttStocks()
stocks_handler = StocksHandler()
utils = Utils()
request_handler = RequestHandler()
dividend_handler = DividendHandler()

balancer = LoadBalancerRegister()


def schedule_job():
    balancer.register_service('invest')


t1 = threading.Thread(target=schedule_job, args=())
t1.start()


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)


@app.route('/fiis', methods=['GET'])
@cross_origin()
def get_fiis_list():
    headers = request.headers
    fiis = fii_handler.get_fiis(headers)
    return json.dumps(fiis, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/stocks-br', methods=['GET'])
@cross_origin()
def get_stocks_br():
    headers = request.headers
    stocks = stocks_handler.get_stocks(1, headers)
    return json.dumps(stocks, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/bdrs', methods=['GET'])
@cross_origin()
def get_bdrs():
    headers = request.headers
    stocks = stocks_handler.get_stocks(4, headers)
    return json.dumps(stocks, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/founds', methods=['GET'])
@cross_origin()
def get_founds():
    headers = request.headers
    stocks = stocks_handler.get_founds(15, headers)
    return json.dumps(stocks, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/investment-facts', methods=['POST'])
@cross_origin()
def investment_fact():
    headers = request.headers
    dumps = json.dumps(investment_handler.investment_facts(request.get_json(), headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investment-facts', methods=['GET'])
@cross_origin()
def get_investment_facts():
    headers = request.headers
    dumps = json.dumps(investment_handler.get_investment_facts(request.args['ticker'], headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investment-facts-labels', methods=['GET'])
@cross_origin()
def get_investment_facts_labels():
    headers = request.headers
    dumps = json.dumps(investment_handler.get_investment_facts_labels(headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/resume-invest', methods=['GET'])
@cross_origin()
def get_resume_invest():
    headers = request.headers
    args = request.args
    dumps = json.dumps(investment_handler.get_resume_invest(args, headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/resume-invest-grafic', methods=['GET'])
@cross_origin()
def get_resume_invest_grafic():
    headers = request.headers
    args = request.args
    dumps = json.dumps(investment_handler.get_resume_invest_grafic(args, headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/add-resume-invest-period', methods=['GET'])
@cross_origin()
def add_resume_invest_period():
    headers = request.headers
    args = request.args
    investment_handler.add_resumes_period(args, headers)
    return {}, 200, {'Content-Type': 'application/json'}


@app.route('/investment-calc', methods=['POST'])
@cross_origin()
def investment_cal():
    headers = request.headers
    dumps = json.dumps(investment_handler.investment_calc(request.get_json(), headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/save-aplly-stock', methods=['POST'])
@cross_origin()
def save_aplly_stock():
    try:
        headers = request.headers
        dumps = json.dumps(investment_handler.save_aplly_stock(request.get_json(), headers))
        return dumps, 200, {'Content-Type': 'application/json'}
    except Exception as e:
        msg = {'error': str(e)}
        print(e)
        return json.dumps(msg), 500, {'Content-Type': 'application/json'}


@app.route('/last-investment-calc', methods=['GET'])
@cross_origin()
def last_investment_cal():
    headers = request.headers
    dumps = json.dumps(investment_handler.last_investment_calc(headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investment-movement', methods=['POST'])
@cross_origin()
def add_movement():
    headers = request.headers
    args = request.args
    dumps = json.dumps(investment_handler.add_movement(request.get_json(), headers))
    # get_investments()
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investment-movements', methods=['POST'])
@cross_origin()
def add_movements():
    headers = request.headers
    args = request.args
    dumps = json.dumps(investment_handler.add_movements(request.get_json(), headers))
    # get_investments()
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/dividend-movement', methods=['POST'])
@cross_origin()
def add_dividend():
    dumps = json.dumps(dividend_handler.add_dividend(request.headers))
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
        time.sleep(1)
        balancer.lock_unlock('invest')
        request_handler.inform_to_client("Investments refresh requested", "investments", headers,
                                         "Investments refresh requested")
        consolidated = investment_handler.buy_sell_indication(args, headers)
        request_handler.inform_to_client("{}", "investments", headers, "Investments refresh completed")
        consolidated = json.dumps(consolidated, cls=Encoder, ensure_ascii=False)
        balancer.lock_unlock('invest', False)
        return consolidated, 200, {'Content-Type': 'application/json'}
    except Exception as e:
        msg = {'error': str(e)}
        print(e)
        balancer.lock_unlock('invest', False)
        return json.dumps(msg), 500, {'Content-Type': 'application/json'}


@app.route('/att-prices', methods=['POST'])
def att_prices():
    if utils.work_day():
        print('att_prices requested')
        headers = request.headers
        threading.Thread(target=att_prices_thr, args=(headers,)).start()
    return "{}", 200, {'Content-Type': 'application/json'}


@app.route('/att-user-dividends-info', methods=['POST'])
def att_user_dividends_info():
    print('att-dividends-info requested')
    headers = request.headers
    att_stocks.att_user_dividends_info(headers)
    return "{}", 200, {'Content-Type': 'application/json'}


@app.route('/att-map-dividends', methods=['POST'])
def att_dividends_info():
    print('att-dividends-info requested')
    headers = request.headers
    att_stocks.att_dividends_info(headers)
    return "{}", 200, {'Content-Type': 'application/json'}


def att_prices_thr(headers):
    time.sleep(1)
    balancer.lock_unlock('invest')
    att_stocks.att_prices(headers)
    balancer.lock_unlock('invest', False)


@app.route('/att-express', methods=['POST'])
def att_express():
    if (utils.work_day() and utils.work_time()) or request.headers.get('force') == 'true':
        print("att_express requested")
        headers = request.headers
        threading.Thread(target=att_stocks.att_expres, args=(headers,)).start()
    return "{}", 200, {'Content-Type': 'application/json'}


@app.route('/att-bdr', methods=['POST'])
def att_bdr():
    if utils.work_day():
        print("att_bdr requested")
        headers = request.headers
        threading.Thread(target=att_bdr_thr, args=(headers,)).start()

    return "{}", 200, {'Content-Type': 'application/json'}


def att_bdr_thr(headers):
    time.sleep(1)
    balancer.lock_unlock('invest')
    att_stocks.att_bdr(headers)
    balancer.lock_unlock('invest', False)


@app.route('/att-full', methods=['POST'])
def att_full():  # put application's code here
    print("att_full requested")
    headers = request.headers
    args = request.args
    threading.Thread(target=att_full_thr, args=(headers,)).start()
    return "{}", 200, {'Content-Type': 'application/json'}


def att_full_thr(headers):
    time.sleep(1)
    balancer.lock_unlock('invest')
    att_stocks.att_full(headers)
    balancer.lock_unlock('invest', False)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
