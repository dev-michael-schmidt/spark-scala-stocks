from airflow import DAG
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def hello_world():
    print('hello world')

with DAG(dag_id="hello_world_dag",
         start_date=datetime(2024,6,1),
         schedule_interval="@hourly",
         catchup=False) as dag:


    initialize = SparkSubmitOperator(application='doo')