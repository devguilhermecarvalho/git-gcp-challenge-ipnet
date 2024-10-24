import os
import logging
from google.cloud import storage

class CloudStorageLoader:
    def __init__(self, config: dict):
        self.client = storage.Client()
        self.bucket_name = config["bucket_name"]
        self.folders = config.get("create_folders", [])

    def get_bucket_environment(self):
        """Verifica e cria o bucket e as pastas necessárias em uma única função."""
        try:
            # Verifica ou cria o bucket
            try:
                bucket = self.client.get_bucket(self.bucket_name)
            except Exception:
                bucket = self.client.create_bucket(self.bucket_name)

            # Verifica ou cria as pastas
            for folder in self.folders:
                blobs = list(self.client.list_blobs(bucket, prefix=f"{folder}/", max_results=1))
                if blobs:
                else:
                    blob = bucket.blob(f"{folder}/")
                    blob.upload_from_string("")  # Cria um 'blob' vazio para simular a pasta
                    logger.info(f"Pasta '{folder}' criada no bucket '{self.bucket_name}'.")
        except Exception as e:
            raise
    
    
    def get_bucket_environment(self):

    def upload_files(self, source_directory: str, destination_folder: str):
        if not os.path.exists(source_directory):
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
