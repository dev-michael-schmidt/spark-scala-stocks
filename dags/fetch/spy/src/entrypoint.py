from datetime import datetime, timedelta
from urllib.parse import urlencode
import time

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator

ROOT_URL = 'https://query1.finance.yahoo.com/v7/finance/download/'
SYMBOL = 'SPY'
interval = '1d'  # one of ['1d', '5d', '1wk', '1mo', '3mo']

start_date = datetime.strptime('2019-01-01', '%Y-%m-%d')
end_date = datetime.today() - timedelta(days=9)
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


@task(task_id="fetch-spy-task")
def read_api(ds=None, **kwargs):
    print('foo')
    df = pd.read_csv(f'{ROOT_URL}{SYMBOL}?{urlencode(payload)}')
    print(df.head(10))
    time.sleep(10)

run_this = read_api()

# with DAG(
#         dag_id="mike_utility-box.home.lan",
#         start_date=datetime(2024, 8, 2),
#         schedule="@daily",
# ) as dag:
#     run_me = DockerOperator(
#         task_id="fetch-spy-task",
#         image="fetch-spy",
#         command=["python", "entrypoint.py"],
#         container_name="fetch-spy-con",
#         docker_url="ssh://mike@utility-box.home.lan",
#     )
#     run_me
#
# read_api()


# run_this = PythonOperator(task_id="fetch-spy-task",
#                           python_callable=read_api
#                           )
#
# run_this
