FROM python:3.9-slim

WORKDIR /app
RUN mkdir -p silver_layer bronze_layer gold_layer

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8080

CMD /bin/bash -c "gunicorn -b 0.0.0.0:8080 main:app & sleep 5 && cd /app/dbt_transformations && dbt run && fg"