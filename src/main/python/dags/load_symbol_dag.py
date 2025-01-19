import os

from airflow import DAG
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator

from dotenv import load_dotenv
load_dotenv()

# env_vars = {
#     'APP_NAME': os.environ.get('APP_NAME'),
#     'P1': os.environ.get('P1'),
#     'P2': os.environ.get('P2'),
#     'INTERVAL': os.environ.get('INTERVAL'),
#     'SYMBOL': os.environ.get('SYMBOL'),
#     'EVENTS': os.environ.get('EVENTS'),
#     'POSTGRES_USER': os.environ.get('POSTGRES_USER'),
#     'POSTGRES_PASSWORD': os.environ.get('POSTGRES_PASSWORD'),
#     'POSTGRES_DB': os.environ.get('POSTGRES_DB'),
#     'TABLE': os.environ.get('SYMBOL'),
#     'DATABASE_MODE': os.environ.get('DATABASE_MODE'),
#     'DB_DRIVER': os.environ.get('DATABASE_DRIVER'),
#     'HADOOP_CONF_DIR': 'not-used',
#     'YARN_CONF_DIR': 'not-used',
# }

with DAG(dag_id="spark_submit_load_symbol",
     start_date=datetime(2024, 6, 1),
     schedule="@daily",
     catchup=False) as dag:

    spark_submit_load_symbol = SparkSubmitOperator(task_id="spark_submit_spy",
                                                   application='/opt/airflow/jars/spark-scala-stocks-1.0.jar',
                                                   # env_vars=env_vars,
                                                   )

    # docker build -t sss-ls-1.0 .
    # load_docker_spy = DockerOperator(task_id='docker_load_spy',
    #                              image='sss-ls-1.0',
    #                              command='java -jar sss-ls-1.0.jar',
    #                              container_name='sss-docker-load',
    #                              environment=env_vars,
    #                              network_mode='container:spark-scala-stocks_default',
    #                              auto_remove='success')

spark_submit_load_symbol #  >> load_docker_spy
