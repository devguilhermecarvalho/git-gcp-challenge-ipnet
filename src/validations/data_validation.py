# src/validations/data_validation.py
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
                    future.result()
                except Exception as e:
                    error_message = f"Erro ao validar o DataFrame '{file_name}': {e}"
                    self.messages.append(error_message)
                    raise e

    def _validate_dataframe(self, file_name, df):
        if df.empty:
            error_message = f"O DataFrame '{file_name}' está vazio."
            self.messages.append(error_message)
            raise ValueError(error_message)
        self.validate_headers(df, file_name)
        if df.isnull().values.any():
            warning_message = f"DataFrame '{file_name}' contém valores nulos."
            self.messages.append(warning_message)

    def validate_headers(self, df: pd.DataFrame, file_name: str):
        if all(isinstance(col, (int, float)) or str(col).isdigit() for col in df.columns):
            num_columns = df.shape[1]
            generic_headers = [f'column{i+1}' for i in range(num_columns)]
            df.columns = generic_headers
            message = f"Headers genéricos aplicados ao DataFrame '{file_name}'."
            self.messages.append(message)
        else:
            df.columns = df.columns.map(str).str.strip()
            message = f"Headers do DataFrame '{file_name}' validados."
            self.messages.append(message)
