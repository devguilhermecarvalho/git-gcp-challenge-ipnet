import pandas as pd

from google.cloud import storage
from google.cloud import bigquery

client = storage.Client()
bucket = client.get_bucket('datalake_challenge')

filename = f"bronze_layer/hackers.csv"

print(f"Arquivo carregado no GCS: {filename}")