import pandas as pd
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor

class BigQueryLoader:
    def __init__(self, project_id: str, parallelism_config: dict[str, int]):
        self.client = bigquery.Client(project=project_id)
        self.messages = []
        self.max_workers_io = parallelism_config.get('max_workers_io', 5)

    def create_dataset_if_not_exists(self, dataset_id: str):
        dataset_ref = bigquery.Dataset(f"{self.client.project}.{dataset_id}")
        try:
            self.client.get_dataset(dataset_ref)
            message = f"Dataset '{dataset_id}' já existe."
            self.messages.append(message)
        except Exception:
            self.client.create_dataset(dataset_ref)
            message = f"Dataset '{dataset_id}' criado com sucesso."
            self.messages.append(message)

    def load_dataframes(self, dataframes: dict[str, pd.DataFrame], dataset_id: str):
        with ThreadPoolExecutor(max_workers=self.max_workers_io) as executor:
            futures = {executor.submit(self.load_dataframe, df, dataset_id, file_name.split('.')[0]): file_name for file_name, df in dataframes.items()}
            for future in futures:
                table_id = futures[future]
                try:
                    future.result()
                except Exception as e:
                    error_message = f"Erro ao carregar a tabela '{table_id}': {e}"
                    self.messages.append(error_message)

    def load_dataframe(self, df: pd.DataFrame, dataset_id: str, table_id: str):
        df = self.validate_dataframe(df)
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        message = f"Dados carregados na tabela '{table_id}' com sucesso."
        self.messages.append(message)

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.map(str).str.strip().str.replace(' ', '_').str.replace(',', '_')
        for column in df.columns:
            if pd.api.types.is_object_dtype(df[column]):
                df[column] = df[column].astype(str)
            elif pd.api.types.is_integer_dtype(df[column]):
                df[column] = df[column].astype('Int64')
            elif pd.api.types.is_float_dtype(df[column]):
                df[column] = df[column].astype(float)
            elif pd.api.types.is_bool_dtype(df[column]):
                df[column] = df[column].astype(bool)
            else:
                df[column] = df[column].astype(str)
        return df
