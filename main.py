import os
import yaml
import logging
import shutil

from flask import Flask, jsonify

from validations.file_validation import FileValidation
from src.data_ingestion import DataIngestion
from validations.data_validation import DataValidation

from src.loaders.bigquery_loader import BigQueryLoader
from src.loaders.cloud_storage_loader import CloudStorageLoader 

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

def run_etl():
    with open('configs/gcp_config.yaml', 'r') as f:
        configs = yaml.safe_load(f)
    
    project_id = configs['project_id']
    dataset_id = configs['dataset_id']
    bucket_name = configs['bucket_name']
    bronze_layer_path = configs['bronze_layer_path']
    silver_layer_path = configs['silver_layer_path']
    gold_layer_path = configs['gold_layer_path']

    for path in [bronze_layer_path, silver_layer_path, gold_layer_path]:
        if not os.path.exists(path):
            os.makedirs(path)
            logging.info(f"Diretório '{path}' criado.")

    cloud_storage_loader = CloudStorageLoader(bucket_name)

    cloud_storage_loader.download_files_from_bucket('bronze_layer/', bronze_layer_path)

    validator = FileValidation()
    validator.validate_and_process_files()

    data_ingestion = DataIngestion()
    dataframes = data_ingestion.read_data(silver_layer_path)


    data_validation = DataValidation(dataframes)
    data_validation.validate_data()

    bq_loader = BigQueryLoader(project_id)
    bq_loader.create_dataset_if_not_exists(dataset_id)

    # Carrega os DataFrames no BigQuery, criando tabelas para cada arquivo processado.
    for file_name, df in dataframes.items():
        table_id = file_name.split('.')[0]  # Define o nome da tabela a partir do nome do arquivo.
        bq_loader.load_dataframe(df, dataset_id, table_id)  # Faz o upload para o BigQuery.
        logging.info(f"Tabela '{table_id}' carregada com sucesso.")  # Registra o sucesso.

    # Faz o upload dos dados para a camada silver no Cloud Storage.
    cloud_storage_loader.upload_files(silver_layer_path, destination_folder='silver_layer/')

    # Remove os diretórios temporários após o processamento.
    for path in [bronze_layer_path, silver_layer_path, gold_layer_path]:
        if os.path.exists(path):
            shutil.rmtree(path)  # Remove o diretório e seus conteúdos.
            logging.info(f"Diretório '{path}' removido após o processamento.")  # Registra a remoção.

# Endpoint para acionar o processo ETL.
@app.route('/', methods=['GET'])
def trigger_etl_endpoint():
    try:
        run_etl()  # Executa o processo ETL.
        return jsonify({'message': 'Processo ETL concluído com sucesso'}), 200  # Retorna mensagem de sucesso.
    except Exception as e:
        logging.error(f"Erro no processo ETL: {e}")  # Registra o erro em caso de falha.
        return jsonify({'error': str(e)}), 500  # Retorna a mensagem de erro e o código 500.

# Ponto de entrada da aplicação.
if __name__ == '__main__':
    # Executa a aplicação Flask, ouvindo na porta definida pelo ambiente ou na porta 8080.
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))