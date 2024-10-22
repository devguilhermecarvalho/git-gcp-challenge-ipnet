FROM python:3.9-slim

WORKDIR /app

RUN mkdir -p silver_layer bronze_layer gold_layer

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD gunicorn -b 0.0.0.0:8080 main:app && cd dbt_transformations && dbt run
