#!/bin/bash
docker exec -it sss_postgres psql -U postgres -c "CREATE DATABASE stocks;"
docker exec -it sss_postgres psql -U postgres -c "CREATE USER airflow SUPERUSER PASSWORD 'airflow';"
docker exec -it sss_postgres psql -U "ALL PRIVILEGES ON DATABASE stocks TO airflow;"

echo "==================     Help for psql   ========================="
echo "\\dt		: Describe the current database"
echo "\\d [table]	: Describe a table"
echo "\\c		: Connect to a database"
echo "\\h		: help with SQL commands"
echo "\\?		: help with psql commands"
echo "\\q		: quit"
echo "=================================================================="
docker exec -it sss_postgres psql -U airflow -d stocks