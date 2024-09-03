docker exec -it sss_postgres psql -U airflow -W airflow -c "CREATE DATABASE stocks;"
docker exec -it sss_postgres psql -U airflow -W airflow -c \
 "CREATE USER airflow SUPERUSER PASSWORD 'airflow'; \
  GRANT ALL PRIVILEGES ON DATABASE stocks TO airflow;"