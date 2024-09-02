#!/bin/bash

# This appears to not be needed
#docker exec -it sss_postgres psql -U airflow -d stocks -c "CREATE USER airflow SUPERUSER PASSWORD 'airflow';"

# these appear to be needed.  possibly creating the DB and user make the airflow init DB easier
docker exec -it sss_postgres psql -U airflow -d stocks -c "GRANT ALL PRIVILEGES ON DATABASE stocks TO airflow;"
docker exec -id sss_postgres psql -U airflow postgres -c "CREATE DATABASE airflow;"
# in container: CREATE USER airflow
GRANT ALL ON SCHEMA public TO airflow;

#docker exec -it sss_postgres psql -U airflow -c "CREATE DATABASE stocks;"
# user airflow seems to exist
#docker exec -it sss_postgres psql -U airflow -d stocks -c "CREATE USER airflow SUPERUSER PASSWORD 'airflow';"
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
