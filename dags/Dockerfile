FROM spark:3.5.2-scala2.12-java11-python3-ubuntu
LABEL authors="mike"

ENV APP_NAME=local
ENV P1=1719842400
ENV P2=1722520800
ENV INTERVAL=1d
ENV SYMBOL=SPY
ENV EVENTS=history
ENV POSTGRES_USER=airflow

ENV POSTGRES_DB=airflow
ENV POSTGRES_HOST=sss-postgres
ENV PRICES=prices
ENV DB_SAVE_MODE=overwrite
ENV DB_DRIVER=org.postgresql.Driver
ENV AIRFLOW_UID=1000
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@sss-postgres/airflow

ENV HADOOP_CONF_DIR=required-not-used
ENV YARN_CONF_DIR=required-not-used

USER root
RUN apt update && \
    apt upgrade -y && \
    apt install -y postgresql-client && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app
WORKDIR /app

EXPOSE 5432

# TODO parameterize this and use a generic name
COPY ../jars/spark-scala-stock-1.0.jar spark-scala-stock-1.0.jar

# ENTRYPOINT ["/bin/bash"]
ENTRYPOINT ["java", "-jar", "spark-scala-stock-1.0.jar"]
# docker build -t sss-ls-1.0 .
