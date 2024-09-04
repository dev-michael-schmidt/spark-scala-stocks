from airflow import DAG
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator

env_vars = {
    'APP_NAME': 'SparkScalaStocks',
    'P1': "1719842400",
    'P2': "1722520800",
    'INTERVAL': '1d',
    'SYMBOL': 'SPY',
    'EVENTS': 'history',
    'POSTGRES_USER': 'airflow',
    'POSTGRES_PASSWORD': 'airflow',  # TODO: Okay for dev, use secrets in prod
    'POSTGRES_DB': 'airflow',
    'PRICES': 'prices',
    'DB_SAVE_MODE': 'overwrite',
    'DB_DRIVER': 'org.postgresql.Driver',
    'HADOOP_CONF_DIR': 'not-used',
    'YARN_CONF_DIR': 'not-used',
}

with DAG(dag_id="load_symbol",
         start_date=datetime(2024, 6, 1),
         schedule_interval="@hourly",
         catchup=False) as dag:
    load_spark_submit_spy = SparkSubmitOperator(task_id="spark_submit_spy",
                                                application='/opt/airflow/dags/jars/load_spy-1.0.jar',
                                                env_vars=env_vars,
                                                )

    # docker build -t sss-ls-1.0 .
    load_docker_spy = DockerOperator(task_id='docker_load_spy',
                                 image='sss-ls-1.0',
                                 command='java -jar sss-ls-1.0.jar',
                                 container_name='sss-docker-load',
                                 environment=env_vars,
                                 network_mode='container:spark-scala-stocks_default',
                                 auto_remove='success')

    load_spark_submit_spy >> load_docker_spy
