import pandas as pd
import os

from google.cloud import storage
from google.cloud import bigquery

from flask import Flask

app = Flask(__name__)

def run_etl():
    client = storage.Client()
    bucket = client.get_bucket('datalake_challenge')

    filename = f"bronze_layer/hackers.csv"

    print(f"Arquivo carregado no GCS: {filename}")


@app.route('/', methods=['GET'])
def trigger_etl_endpoint():
    run_etl()
    return 'ETL process completed', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
