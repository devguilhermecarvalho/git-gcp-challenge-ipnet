from google.cloud import storage
from typing import Dict
from concurrent.futures import ThreadPoolExecutor

class CloudStorageLoader:
    def __init__(self, bucket_name: str, parallelism_config: Dict[str, int]):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.messages = []
        self.max_workers_io = parallelism_config.get('max_workers_io', 5)

    def download_files_from_bucket(self, source_folder: str) -> Dict[str, bytes]:
        blobs = [blob for blob in self.client.list_blobs(self.bucket, prefix=source_folder) if not blob.name.endswith('/')]
        files = {}
        with ThreadPoolExecutor(max_workers=self.max_workers_io) as executor:
            futures = {executor.submit(self._download_blob, blob, source_folder): blob for blob in blobs}
            for future in futures:
                blob = futures[future]
                try:
                    file_name, file_content = future.result()
                    files[file_name] = file_content
                except Exception as e:
                    error_message = f"Erro ao baixar o arquivo '{blob.name}': {e}"
                    self.messages.append(error_message)
        return files

    def _download_blob(self, blob, source_folder):
        file_name = blob.name.replace(source_folder, '')
        file_content = blob.download_as_bytes()
        message = f"Arquivo '{file_name}' baixado com sucesso."
        self.messages.append(message)
        return file_name, file_content

    def upload_files(self, files: Dict[str, bytes], destination_folder: str):
        with ThreadPoolExecutor(max_workers=self.max_workers_io) as executor:
            futures = {executor.submit(self._upload_file, file_name, file_content, destination_folder): file_name for file_name, file_content in files.items()}
            for future in futures:
                file_name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    error_message = f"Erro ao enviar o arquivo '{file_name}': {e}"
                    self.messages.append(error_message)

    def _upload_file(self, file_name, file_content, destination_folder):
        blob_name = f"{destination_folder}{file_name}"
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(file_content)
        message = f"Arquivo '{file_name}' enviado para '{destination_folder}'."
        self.messages.append(message)