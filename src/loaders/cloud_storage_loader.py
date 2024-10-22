import os
import logging
from google.cloud import storage

class CloudStorageLoader:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.get_or_create_bucket()

    def get_or_create_bucket(self):
        try:
            bucket = self.client.get_bucket(self.bucket_name)
            logging.info(f"Bucket '{self.bucket_name}' já existe.")
        except Exception as e:
            logging.info(f"Bucket '{self.bucket_name}' não encontrado. Criando...")
            bucket = self.client.create_bucket(self.bucket_name)
            logging.info(f"Bucket '{self.bucket_name}' criado com sucesso.")
        return bucket

    def upload_files(self, source_directory: str, destination_folder: str):
        if not os.path.exists(source_directory):
            logging.warning(f"Diretório '{source_directory}' não existe. Nenhum arquivo para fazer upload.")
            return
        
        for root, _, files in os.walk(source_directory):
            for file_name in files:
                local_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(local_path, source_directory)
                blob_path = os.path.join(destination_folder, relative_path)
                blob = self.bucket.blob(blob_path)
                blob.upload_from_filename(local_path)
                logging.info(f"Upload de {file_name} para gs://{self.bucket_name}/{blob_path}")

    def download_files_from_bucket(self, source_folder: str, destination_directory: str):
        blobs = self.client.list_blobs(self.bucket_name, prefix=source_folder)
        os.makedirs(destination_directory, exist_ok=True)
        files_downloaded = False

        for blob in blobs:
            # Ignorar pastas
            if blob.name.endswith('/'):
                continue

            relative_path = os.path.relpath(blob.name, source_folder)
            local_path = os.path.join(destination_directory, relative_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob.download_to_filename(local_path)
            logging.info(f"Download de {blob.name} para {local_path}")
            files_downloaded = True

        if not files_downloaded:
            logging.warning(f"Nenhum arquivo encontrado na pasta '{source_folder}' do bucket '{self.bucket_name}'.")

    def verify_folder_exists(self, folder_path: str):
        blobs = list(self.bucket.list_blobs(prefix=folder_path))
        if not blobs:
            logging.info(f"Pasta '{folder_path}' não existe no bucket '{self.bucket_name}'. Criando...")
            blob = self.bucket.blob(f"{folder_path}/.placeholder")
            blob.upload_from_string('')
            logging.info(f"Pasta '{folder_path}' criada com sucesso.")
