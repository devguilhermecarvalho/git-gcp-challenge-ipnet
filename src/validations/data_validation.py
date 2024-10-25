import pandas as pd
from typing import Dict
from concurrent.futures import ProcessPoolExecutor

class DataValidation:
    def __init__(self, dataframes: Dict[str, pd.DataFrame], parallelism_config: Dict[str, int]):
        self.dataframes = dataframes
        self.messages = []
        self.max_workers_cpu = parallelism_config.get('max_workers_cpu', 4)

    def validate_data(self):
        with ProcessPoolExecutor(max_workers=self.max_workers_cpu) as executor:
            futures = {executor.submit(self._validate_dataframe, file_name, df): file_name for file_name, df in self.dataframes.items()}
            for future in futures:
                file_name = futures[future]
                try:
                    df, messages = future.result()
                    self.dataframes[file_name] = df  # Atualiza o DataFrame no processo principal
                    self.messages.extend(messages)   # Agrega as mensagens
                except Exception as e:
                    error_message = f"Erro ao validar o DataFrame '{file_name}': {e}"
                    self.messages.append(error_message)
                    raise e

    def _validate_dataframe(self, file_name, df):
        messages = []
        if df.empty:
            error_message = f"O DataFrame '{file_name}' está vazio."
            messages.append(error_message)
            raise ValueError(error_message)
        df = self.validate_headers(df, file_name, messages)
        if df.isnull().values.any():
            warning_message = f"DataFrame '{file_name}' contém valores nulos."
            messages.append(warning_message)
        return df, messages

    def validate_headers(self, df: pd.DataFrame, file_name: str, messages: list):
        num_columns = df.shape[1]
        generic_headers = [f'column{i+1}' for i in range(num_columns)]
        df.columns = generic_headers
        message = f"Headers genéricos aplicados ao DataFrame '{file_name}'."
        messages.append(message)
        return df
