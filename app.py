# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import decimal
import json

import psycopg2
from flask import Flask
from flask_cors import CORS, cross_origin

from prometheus_flask_exporter import PrometheusMetrics

from service.FiiHandler import FiiHandler
from service.LoadInfo import load_fiis_info

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


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal): return float(obj)


@app.route('/atualiza-fiis', methods=['POST'])
def hello_world():  # put application's code here
    fii_handler.att_fiis(load_fiis_info())
    return 'Hello World!'


@app.route('/fiis', methods=['GET'])
@cross_origin()
def get_fiis_list():
    fiis = fii_handler.get_fiis()
    return json.dumps(fiis, cls=Encoder), 200, {'Content-Type': 'application/json'}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
