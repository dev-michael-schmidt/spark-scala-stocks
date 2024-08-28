#!/bin/bash
#docker exec -it sss_postgres psql -U postgres -c "CREATE DATABASE stocks;"
# user airflow seems to exist
#docker exec -it sss_postgres psql -U airflow -d stocks -c "CREATE USER airflow SUPERUSER PASSWORD 'airflow';"
#docker exec -it sss_postgres psql -U airflow -d stocks -c "GRANT ALL PRIVILEGES ON DATABASE stocks TO airflow;"
# use schema to create a table (can you "pipe" the schema: like -c "{piped_schema_file(s)}"

echo "==================     Help for psql   ========================="
echo "\\dt		: Describe the current database"
echo "\\d [table]	: Describe a table"
echo "\\c		: Connect to a database"
echo "\\h		: help with SQL commands"
echo "\\?		: help with psql commands"
echo "\\q		: quit"
echo "=================================================================="
docker exec -it sss_postgres psql -U airflow -d stocks
