import os
import yaml
import logging
import shutil

from flask import Flask, jsonify

from src.file_validation import FileValidation
from src.data_ingestion import DataIngestion
from src.data_validation import DataValidation

from src.loaders.bigquery_loader import BigQueryLoader
from src.loaders.cloud_storage_loader import CloudStorageLoader

# Configuração de logging para Cloud Run
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

    # Criar diretórios temporários se não existirem
    for path in [bronze_layer_path, silver_layer_path, gold_layer_path]:
        if not os.path.exists(path):
            os.makedirs(path)
            logging.info(f"Diretório '{path}' criado.")

    # Instanciar o carregador de Cloud Storage
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

    for file_name, df in dataframes.items():
        table_id = file_name.split('.')[0]
        bq_loader.load_dataframe(df, dataset_id, table_id)
        logging.info(f"Tabela '{table_id}' carregada com sucesso.")

    cloud_storage_loader.upload_files(silver_layer_path, destination_folder='silver_layer/')

    for path in [bronze_layer_path, silver_layer_path, gold_layer_path]:
        if os.path.exists(path):
            shutil.rmtree(path)
            logging.info(f"Diretório '{path}' removido após o processamento.")

@app.route('/', methods=['GET'])
def trigger_etl_endpoint():
    try:
        run_etl()
        return jsonify({'message': 'Processo ETL concluído com sucesso'}), 200
    except Exception as e:
        logging.error(f"Erro no processo ETL: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
