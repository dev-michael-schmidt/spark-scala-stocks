FROM apache/airflow
LABEL authors="mike"

ENTRYPOINT ["top", "-b"]