FROM python:latest
EXPOSE 5432
RUN pip install psycopg2
COPY postgres_debug.py postgres_debug.py
ENTRYPOINT ["python", "postgres_debug.py"]