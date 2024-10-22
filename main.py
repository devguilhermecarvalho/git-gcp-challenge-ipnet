import pandas as pd
import os

from google.cloud import storage
from google.cloud import bigquery

def main():
    client = storage.Client()
    bucket = client.get_bucket('datalake_challenge')

    filename = f"bronze_layer/hackers.csv"

    print(f"Arquivo carregado no GCS: {filename}")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Aplicação iniciada na porta {port}. Executando o ETL...")
    main()
