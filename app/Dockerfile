FROM python:3.11-slim-bookworm

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

CMD [ "python", "main.py" ]