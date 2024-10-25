# src/config.py
import yaml

class Config:
    def __init__(self, gcp_config_path='configs/gcp_configs.yaml', project_config_path='configs/project_config.yaml'):
        self.gcp_configs = self._load_yaml_file(gcp_config_path)
        self.project_configs = self._load_yaml_file(project_config_path)

    def _load_yaml_file(self, path):
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    @property
    def project_id(self):
        return self.gcp_configs['project_id']

    @property
    def credentials_path(self):
        return self.gcp_configs['credentials_path']

    @property
    def cloud_storage(self):
        return self.gcp_configs['cloud_storage']

    @property
    def bigquery(self):
        return self.gcp_configs['bigquery']

    @property
    def file_delimiter_mapping(self):
        return self.project_configs.get('file_delimiter_mapping', {})

    @property
    def parallelism_config(self):
        return self.project_configs.get('parallelism', {})
