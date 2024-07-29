FROM python:3.12.4 AS HAutomata

RUN mkdir /app

WORKDIR /app

RUN mkdir /data
RUN mkdir /database
RUN mkdir /database/month
RUN mkdir /database/datamart
RUN mkdir /database/trip
RUN mkdir /logs

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT python -u main.py