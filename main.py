import pandas as pd
import os
import logging
from google.cloud import storage
from google.cloud import bigquery
from flask import Flask, jsonify

# Configuração de logging para Cloud Run
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

def run_etl():
    """Executa o processo ETL e insere um teste no BigQuery."""
    try:
        # Inicializa o cliente do Cloud Storage
        client = storage.Client()
        bucket = client.get_bucket('datalake_challenge')

        # Define o nome do arquivo na camada bronze
        filename = "bronze_layer/hackers.csv"
        blob = bucket.blob(filename)

        if blob.exists():
            logging.info(f"Arquivo encontrado no GCS: {filename}")
        else:
            raise FileNotFoundError(f"Arquivo {filename} não encontrado no GCS.")
        
        # Inserir registro de teste no BigQuery
        insert_test_data_into_bigquery()
        logging.info("Dados inseridos no BigQuery com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante o processo ETL: {e}")
        raise e

def create_bigquery_table():
    """Cria uma tabela no BigQuery se ela não existir."""
    client = bigquery.Client()
    dataset_id = "dataset_challenge_ipnet"
    table_id = f"{dataset_id}.etl_log"

    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("message", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    try:
        client.create_table(table)
        logging.info(f"Tabela {table_id} criada com sucesso.")
    except Exception as e:
        logging.warning(f"Tabela {table_id} já existe ou erro na criação: {e}")

def insert_test_data_into_bigquery():
    """Insere um registro de teste na tabela do BigQuery."""
    client = bigquery.Client()
    table_id = "dataset_challenge_ipnet.etl_log"
    rows_to_insert = [
        {"id": "1", "message": "Teste de inserção", "timestamp": bigquery._helpers.datetime.datetime.now()}
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        raise Exception(f"Erro ao inserir dados: {errors}")

@app.route('/', methods=['GET'])
def trigger_etl_endpoint():
    """Rota para acionar o processo ETL."""
    try:
        create_bigquery_table()
        run_etl()
        return jsonify({'message': 'ETL process completed successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Executa o servidor Flask
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
