FROM python:3.10

WORKDIR /app

COPY gen.py /app
COPY requirements.txt /app

RUN pip install -r requirements.txt

CMD ["python", "gen.py"]