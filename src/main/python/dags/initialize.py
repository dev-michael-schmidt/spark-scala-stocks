from airflow import DAG
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

env_vars = {
    'APP_NAME': 'SparkScalaStocks',
    'P1': "1719842400",
    'P2': "1722520800",
    'INTERVAL': '1d',
    'SYMBOL': 'SPY',
    'EVENTS': 'history',
    'POSTGRES_USER': 'airflow',
    'POSTGRES_PASSWORD': 'airflow',
    'POSTGRES_DB': 'stocks',
    'PRICES': 'prices',
    'DB_SAVE_MODE': 'overwrite',
    'DB_DRIVER': 'org.postgresql.Driver',
}

with DAG(dag_id="hello_world_dag2",
         start_date=datetime(2024, 6, 1),
         schedule_interval="@hourly",
         catchup=False) as dag:
    initialize = SparkSubmitOperator(task_id="BAAAAAZZZZ",
                                     application='/opt/airflow/lib/load_symbol/sss-0.1.jar',
                                     env_vars=env_vars
                                     )
    initialize
