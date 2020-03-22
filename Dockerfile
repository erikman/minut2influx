FROM python:3.8-buster

RUN mkdir -p /app
WORKDIR /app

ADD requirements.txt .
RUN pip3 install -r requirements.txt

ADD * ./

ENTRYPOINT ["python3", "./uploader.py"]
