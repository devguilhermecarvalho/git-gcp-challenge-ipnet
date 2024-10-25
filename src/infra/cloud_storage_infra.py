from google.cloud import storage

class CloudStorageInfra:
    def __init__(self, config_infra: dict):
        self.client = storage.Client()
        self.bucket_name = config_infra.get("bucket_names", [])
        self.folders = config_infra.get("create_folders", [])

    def get_storage_environment(self):
        try:
            bucket = self._get_or_create_bucket()
            self._create_folders_if_no_exist(bucket)
        except Exception as e:
            raise
    
    def _get_or_create_bucket(self):
        try:
            bucket = self.client.get_bucket(self.bucket_name)
        except Exception:
            bucket = self.client.create_bucket(self.bucket_name)
        return bucket
    
    def _create_folders_if_not_exist(self, bucket):
        for folder in self.folders:
            if not self._folder_exists(bucket, folder):
                blob = bucket.blob(f"{folder}/")
                blob.upload_from_string("")

    def _folder_exists(self, bucket, folder: str) -> bool:
        blobs = list(self.client.list_blobs(bucket, prefix=f"{folder}/", max_results=1))
        return bool(blobs)