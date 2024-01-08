import decimal
import json
import threading
import time

from csctracker_py_core.repository.http_repository import cross_origin
from csctracker_py_core.starter import Starter
from csctracker_py_core.utils.utils import Utils

from service.att_stocks import AttStocks
from service.dividend_handler import DividendHandler
from service.fii_handler import FiiHandler
from service.fixed_income import FixedIncome
from service.investment_handler import InvestmentHandler
from service.load_info import LoadInfo
from service.stocks_handler import StocksHandler

starter = Starter()
app = starter.get_app()
remote_repository = starter.get_remote_repository()
http_repository = starter.get_http_repository()

load_info = LoadInfo(
    remote_repository=remote_repository,
    http_repository=http_repository
)
fii_handler = FiiHandler(
    load_info=load_info,
    remote_repository=remote_repository,
    http_repository=http_repository
)
stocks_handler = StocksHandler(
    remote_repository=remote_repository,
    http_repository=http_repository
)
fixed_income_handler = FixedIncome(
    stock_handler=stocks_handler,
    remote_repository=remote_repository,
    http_repository=http_repository
)
dividend_handler = DividendHandler(
    remote_repository=remote_repository,
    http_repository=http_repository
)
investment_handler = InvestmentHandler(
    fii_handler=fii_handler,
    stock_handler=stocks_handler,
    fixed_income_handler=fixed_income_handler,
    dividend_handler=dividend_handler,
    remote_repository=remote_repository,
    http_repository=http_repository
)
att_stocks = AttStocks(
    load_info=load_info,
    fixed_income_handler=fixed_income_handler,
    investment_handler=investment_handler,
    remote_repository=remote_repository,
    http_repository=http_repository
)


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)


@app.route('/fiis', methods=['GET'])
@cross_origin()
def get_fiis_list():
    headers = http_repository.get_headers()
    fiis = fii_handler.get_fiis(headers)
    return json.dumps(fiis, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/stocks-br', methods=['GET'])
@cross_origin()
def get_stocks_br():
    headers = http_repository.get_headers()
    stocks = stocks_handler.get_stocks(1, headers)
    return json.dumps(stocks, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/bdrs', methods=['GET'])
@cross_origin()
def get_bdrs():
    headers = http_repository.get_headers()
    stocks = stocks_handler.get_stocks(4, headers)
    return json.dumps(stocks, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/founds', methods=['GET'])
@cross_origin()
def get_founds():
    headers = http_repository.get_headers()
    stocks = stocks_handler.get_founds(15, headers)
    return json.dumps(stocks, cls=Encoder), 200, {'Content-Type': 'application/json'}


@app.route('/investment-facts', methods=['POST'])
@cross_origin()
def investment_fact():
    headers = http_repository.get_headers()
    dumps = json.dumps(investment_handler.investment_facts(http_repository.get_json_body(), headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investment-facts', methods=['GET'])
@cross_origin()
def get_investment_facts():
    headers = http_repository.get_headers()
    dumps = json.dumps(investment_handler.get_investment_facts(http_repository.get_args()['ticker'], headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investment-facts-labels', methods=['GET'])
@cross_origin()
def get_investment_facts_labels():
    headers = http_repository.get_headers()
    dumps = json.dumps(investment_handler.get_investment_facts_labels(headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/resume-invest', methods=['GET'])
@cross_origin()
def get_resume_invest():
    headers = http_repository.get_headers()
    args = http_repository.get_args()
    dumps = json.dumps(investment_handler.get_resume_invest(args, headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/resume-invest-grafic', methods=['GET'])
@cross_origin()
def get_resume_invest_grafic():
    headers = http_repository.get_headers()
    args = http_repository.get_args()
    dumps = json.dumps(investment_handler.get_resume_invest_grafic(args, headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/add-resume-invest-period', methods=['GET'])
@cross_origin()
def add_resume_invest_period():
    headers = http_repository.get_headers()
    args = http_repository.get_args()
    threading.Thread(target=add_resume_invest_period_tr, args=(args, headers,)).start()
    return {}, 200, {'Content-Type': 'application/json'}


def add_resume_invest_period_tr(args, headers):
    investment_handler.add_resumes_period(args, headers)


@app.route('/re-add-resume-invest-period', methods=['GET'])
@cross_origin()
def re_add_resume_invest_period():
    headers = http_repository.get_headers()
    args = http_repository.get_args()
    threading.Thread(target=re_add_resume_invest_period_tr, args=(args, headers,)).start()
    return {}, 200, {'Content-Type': 'application/json'}


def re_add_resume_invest_period_tr(args, headers):
    investment_handler.re_add_resumes_period(args, headers)


@app.route('/investment-calc', methods=['POST'])
@cross_origin()
def investment_cal():
    headers = http_repository.get_headers()
    get_json = http_repository.get_json_body()
    dumps = json.dumps(investment_handler.investment_calc(get_json, headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/save-aplly-stock', methods=['POST'])
@cross_origin()
def save_aplly_stock():
    try:
        headers = http_repository.get_headers()
        dumps = json.dumps(investment_handler.save_aplly_stock(http_repository.get_json_body(), headers))
        return dumps, 200, {'Content-Type': 'application/json'}
    except Exception as e:
        msg = {'error': str(e)}
        print(e)
        return json.dumps(msg), 500, {'Content-Type': 'application/json'}


@app.route('/save-all-aplly-stock', methods=['POST'])
@cross_origin()
def save_all_aplly_stock():
    try:
        headers = http_repository.get_headers()
        dumps = json.dumps(investment_handler.save_all_aplly_stock(http_repository.get_json_body(), headers))
        return dumps, 200, {'Content-Type': 'application/json'}
    except Exception as e:
        msg = {'error': str(e)}
        print(e)
        return json.dumps(msg), 500, {'Content-Type': 'application/json'}


@app.route('/last-investment-calc', methods=['GET'])
@cross_origin()
def last_investment_cal():
    headers = http_repository.get_headers()
    dumps = json.dumps(investment_handler.last_investment_calc(headers))
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investment-movement', methods=['POST'])
@cross_origin()
def add_movement():
    headers = http_repository.get_headers()
    dumps = json.dumps(investment_handler.add_movement(http_repository.get_json_body(), headers))
    # get_investments()
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investment-movements', methods=['POST'])
@cross_origin()
def add_movements():
    headers = http_repository.get_headers()
    dumps = json.dumps(investment_handler.add_movements(http_repository.get_json_body(), headers))
    # get_investments()
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/dividend-movement', methods=['POST'])
@cross_origin()
def add_dividend():
    dumps = json.dumps(dividend_handler.add_dividend(http_repository.get_headers()))
    get_investments()
    return dumps, 200, {'Content-Type': 'application/json'}


@app.route('/investments', methods=['GET'])
@cross_origin()
def get_investments():
    try:
        headers = http_repository.get_headers()
        args = http_repository.get_args()
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
        Utils.inform_to_client("Investments refresh requested", "investments", headers,
                                         "Investments refresh requested")
        consolidated = investment_handler.buy_sell_indication(args, headers)
        Utils.inform_to_client("{}", "investments", headers, "Investments refresh completed")
        consolidated = json.dumps(consolidated, cls=Encoder, ensure_ascii=False)
        return consolidated, 200, {'Content-Type': 'application/json'}
    except Exception as e:
        msg = {'error': str(e)}
        print(e)
        return json.dumps(msg), 500, {'Content-Type': 'application/json'}


@app.route('/att-prices', methods=['POST'])
def att_prices():
    if Utils.work_day():
        print('att_prices requested')
        headers = http_repository.get_headers()
        threading.Thread(target=att_prices_thr, args=(headers,)).start()
    return "{}", 200, {'Content-Type': 'application/json'}


@app.route('/att-user-dividends-info', methods=['POST'])
def att_user_dividends_info():
    print('att-dividends-info requested')
    headers = http_repository.get_headers()
    threading.Thread(target=att_user_dividends_info_tr, args=(headers,)).start()
    return "{}", 200, {'Content-Type': 'application/json'}


def att_user_dividends_info_tr(headers):
    att_stocks.att_user_dividends_info(headers)


@app.route('/att-map-dividends', methods=['POST'])
def att_dividends_info():
    print('att-dividends-info requested')
    headers = http_repository.get_headers()
    threading.Thread(target=att_dividends_info_tr, args=(headers,)).start()
    return "{}", 200, {'Content-Type': 'application/json'}


def att_dividends_info_tr(headers):
    att_stocks.att_dividends_info(headers)


def att_prices_thr(headers):
    time.sleep(1)
    att_stocks.att_prices(headers)


@app.route('/att-express', methods=['POST'])
def att_express():
    if (Utils.work_day() and Utils.work_time()) or http_repository.get_headers().get('force') == 'true':
        print("att_express requested")
        headers = http_repository.get_headers()
        threading.Thread(target=att_stocks.att_expres, args=(headers,)).start()
    return "{}", 200, {'Content-Type': 'application/json'}


@app.route('/att-bdr', methods=['POST'])
def att_bdr():
    if Utils.work_day():
        print("att_bdr requested")
        headers = http_repository.get_headers()
        threading.Thread(target=att_bdr_thr, args=(headers,)).start()

    return "{}", 200, {'Content-Type': 'application/json'}


def att_bdr_thr(headers):
    time.sleep(1)
    att_stocks.att_bdr(headers)


@app.route('/att-full', methods=['POST'])
def att_full():  # put application's code here
    print("att_full requested")
    headers = http_repository.get_headers()
    threading.Thread(target=att_full_thr, args=(headers,)).start()
    return "{}", 200, {'Content-Type': 'application/json'}


def att_full_thr(headers):
    time.sleep(1)
    att_stocks.att_full(headers)


starter.start()
