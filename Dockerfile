FROM apache/airflow:latest

RUN mkdir -p /opt/airflow/libs \
    && pip install apache-airflow-providers-apache-spark

ENTRYPOINT [ "airflow", "scheduler"]

