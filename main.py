import pandas as pd
import os
from google.cloud import storage
from google.cloud import bigquery
from flask import Flask, jsonify

app = Flask(__name__)

def run_etl():
    """Executa o processo ETL."""
    try:
        # Inicializa o cliente do Cloud Storage
        client = storage.Client()
        bucket = client.get_bucket('datalake_challenge')

        # Define o nome do arquivo na camada bronze
        filename = "bronze_layer/hackers.csv"
        blob = bucket.blob(filename)

        # Verifica se o arquivo existe
        if blob.exists():
            print(f"Arquivo encontrado no GCS: {filename}")
        else:
            raise FileNotFoundError(f"Arquivo {filename} não encontrado no GCS.")
    except Exception as e:
        print(f"Erro durante o processo ETL: {e}")
        raise e

@app.route('/', methods=['GET'])
def trigger_etl_endpoint():
    """Rota para acionar o processo ETL."""
    try:
        run_etl()
        return jsonify({'message': 'ETL process completed successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Executa o servidor Flask
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
