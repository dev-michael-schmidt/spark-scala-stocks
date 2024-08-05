FROM python:3.12
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN mkdir app

COPY spy/src /app

WORKDIR /app
CMD ["python", "entrypoint.py"]
