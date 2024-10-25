from src.validations.file_validation import FileValidation
from src.data_ingestion import DataIngestion
from src.validations.data_validation import DataValidation
from src.services.bigquery_service import BigQueryLoader
from src.services.cloud_storage_service import CloudStorageLoader

class ETLFactory:
    def __init__(self, config):
        self.config = config
        self.project_id = config.project_id
        self.bucket_name = config.cloud_storage['bucket_name']
        self.dataset_id = config.bigquery['dataset_id']
        self.file_delimiter_mapping = config.file_delimiter_mapping
        self.parallelism_config = config.parallelism_config

        self.cloud_storage_loader = CloudStorageLoader(self.bucket_name, self.parallelism_config)
        self.bq_loader = BigQueryLoader(self.project_id, self.parallelism_config)

    def run_etl(self):
        messages = []

        # Baixar arquivos da camada bronze
        data_files = self.cloud_storage_loader.download_files_from_bucket('bronze_layer/')
        messages.extend(self.cloud_storage_loader.messages)

        if not data_files:
            messages.append("Nenhum arquivo para processar.")
            return messages

        # Validar e processar arquivos
        file_validator = FileValidation(self.parallelism_config)
        valid_files = file_validator.validate_and_process_files(data_files)
        messages.extend(file_validator.messages)

        # Ingestão de dados
        data_ingestion = DataIngestion(self.file_delimiter_mapping, self.parallelism_config)
        dataframes = data_ingestion.read_data(valid_files)
        messages.extend(data_ingestion.messages)

        # Validação dos dados
        data_validator = DataValidation(dataframes, self.parallelism_config)
        data_validator.validate_data()
        messages.extend(data_validator.messages)

        # Carregar para o BigQuery
        self.bq_loader.create_dataset_if_not_exists(self.dataset_id)
        messages.extend(self.bq_loader.messages)

        self.bq_loader.load_dataframes(dataframes, self.dataset_id)
        messages.extend(self.bq_loader.messages)

        # Fazer upload dos arquivos processados para a camada silver
        self.cloud_storage_loader.upload_files(valid_files, 'silver_layer/')
        messages.extend(self.cloud_storage_loader.messages)

        return messages
