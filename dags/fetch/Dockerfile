FROM python:3.12
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan utility-box.home.lan >> ~/.ssh/known_hosts

RUN mkdir app

COPY spy/src /app

WORKDIR /app
CMD ["python", "remote_docker.py"]
