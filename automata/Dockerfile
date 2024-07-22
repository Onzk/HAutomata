FROM python:3.12.4 AS HAutomata

RUN mkdir /app

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT python -u main.py