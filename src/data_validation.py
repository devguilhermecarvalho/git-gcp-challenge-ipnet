import yaml
import logging
import pandas as pd
from typing import Dict

with open('configs/gcp_config.yaml', 'r') as f:
    configs = yaml.safe_load(f)

class DataValidation:
    def __init__(self, dataframes: Dict[str, pd.DataFrame]):
        self.dataframes = dataframes

    def validate_data(self):
        for file_name, df in self.dataframes.items():
            try:
                if df.empty:
                    raise ValueError(f"O DataFrame do arquivo '{file_name}' está vazio.")
                df = self.validate_headers(df, file_name)
                if df.isnull().values.any():
                    logging.info(f"Aviso: O DataFrame do arquivo '{file_name}' contém valores nulos.")
                else:
                    logging.info(f"O DataFrame do arquivo '{file_name}' passou nas validações.")
            except Exception as e:
                logging.error(f"Erro na validação do arquivo '{file_name}': {e}")

    def validate_headers(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        if all(isinstance(col, (int, float)) or str(col).isdigit() for col in df.columns):
            logging.info(f"Headers numéricos detectados no arquivo '{file_name}'. Atribuindo headers genéricos.")
            num_columns = df.shape[1]
            generic_headers = [f'column{i+1}' for i in range(num_columns)]
            df.columns = generic_headers
        else:
            df.columns = df.columns.map(str)
        return df
