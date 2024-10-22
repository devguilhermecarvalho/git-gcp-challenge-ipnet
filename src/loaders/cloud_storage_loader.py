# src/loaders/cloud_storage_loader.py
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

    def upload_files(self, source_directory: str, destination_folder: str = 'silver_layer/'):
        if not os.path.exists(source_directory):
            os.makedirs(source_directory)
            logging.info(f"Diretório '{source_directory}' criado.")
        
        for root, _, files in os.walk(source_directory):
            for file_name in files:
                local_path = os.path.join(root, file_name)
                blob_path = os.path.join(destination_folder, file_name)
                blob = self.bucket.blob(blob_path)
                blob.upload_from_filename(local_path)
                logging.info(f"Upload de {file_name} para gs://{self.bucket_name}/{blob_path}")

    def verify_folder_exists(self, folder_path: str):
        blobs = list(self.bucket.list_blobs(prefix=folder_path))
        if not blobs:
            logging.info(f"Pasta '{folder_path}' não existe. Criando...")
            blob = self.bucket.blob(f"{folder_path}/.placeholder")
            blob.upload_from_string('')
            logging.info(f"Pasta '{folder_path}' criada com sucesso.")