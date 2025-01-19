set -o allexport
source .env
# docker-compose build

# docker build --tag sss/airflow-2.10-java11 .

set +o allexport
mvn clean compile assembly:single