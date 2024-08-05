
from datetime import datetime, timedelta
from urllib.parse import urlencode
import time

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

ROOT_URL = 'https://query1.finance.yahoo.com/v7/finance/download/'
SYMBOL = 'SPY'
interval = '1d'  # one of ['1d', '5d', '1wk', '1mo', '3mo']

START_YEAR, START_MONTH, START_DAY = 2024, 7, 1

start_date = datetime.strptime(f'{START_YEAR}-{START_MONTH}-{START_DAY}', '%Y-%m-%d')
end_date = datetime.today() - timedelta(days=3)
start_ts = int(datetime.timestamp(start_date))
end_ts = int(datetime.timestamp(end_date))

payload = {
    'period1': start_ts,
    'period2': end_ts,
    'interval': interval,
    'events': 'history',
}

assert payload['period1'] < payload['period2'], '\'End\' date must be before \'Start\' date'
assert payload['period2'] < datetime.timestamp(datetime.today()), '\'End\' date must be in the past'


def read_api():
    print('foo')
    df = pd.read_csv(f'{ROOT_URL}{SYMBOL}?{urlencode(payload)}')
    print(df.head(10))
    time.sleep(10)
    df

# run_this = read_api()

with DAG(
        dag_id="fetch-spy",
        start_date=datetime(START_YEAR, START_MONTH, START_DAY),
        schedule="@daily",
) as dag:
    run_me = PythonOperator(python_callable=read_api, task_id='bar')

