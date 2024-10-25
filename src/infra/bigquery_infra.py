from google.cloud import bigquery

class BigQueryInfra:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def create_dataset_if_not_exists(self, dataset_id: str):
        dataset_ref = f"{self.client.project}.{dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
        except Exception as e:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)