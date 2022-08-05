# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import psycopg2
from flask import Flask

from prometheus_flask_exporter import PrometheusMetrics

from service.FiiHandller import attFiis
from service.LoadInfo import load_fiis_info


app = Flask(__name__)

metrics = PrometheusMetrics(app, group_by='endpoint')

conn = psycopg2.connect(
    host="postgres",
    database="postgres",
    user="postgres",
    password="postgres")


@app.route('/atualiza_fiis', methods=['POST'])
def hello_world():  # put application's code here
    attFiis(load_fiis_info())
    return 'Hello World!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
