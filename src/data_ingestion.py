import pandas as pd
import os
from typing import Dict
from abc import ABC, abstractmethod
import yaml
import logging

with open('configs/gcp_config.yaml', 'r') as f:
    configs = yaml.safe_load(f)

silver_layer_path = configs['silver_layer_path']

class Reader(ABC):
    @abstractmethod
    def read(self, file_path: str, delimiter=None) -> pd.DataFrame:
        pass

class CSVReader(Reader):
    def read(self, file_path: str, delimiter=None) -> pd.DataFrame:
        try:
            if delimiter:
                df = pd.read_csv(file_path, sep=delimiter, engine='python', header=None)
            else:
                df = pd.read_csv(file_path, sep=None, engine='python', header=None)
            df = self._apply_generic_headers(df)
            return df
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo {file_path}: {e}")
            raise e

    def _apply_generic_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        num_columns = df.shape[1]
        generic_headers = [f'column{i+1}' for i in range(num_columns)]
        df.columns = generic_headers
        return df

class TSVReader(Reader):
    def read(self, file_path: str, delimiter=None) -> pd.DataFrame:
        return pd.read_csv(file_path, sep='\t', header=None)

class ExcelReader(Reader):
    def read(self, file_path: str, delimiter=None) -> pd.DataFrame:
        return pd.read_excel(file_path, header=None)
    
class ReaderFactory:
    _readers = {
        '.csv': CSVReader(),
        '.tsv': TSVReader(),
        '.txt': CSVReader(),
        '.xlsx': ExcelReader(),
        '.xls': ExcelReader()
    }

    @classmethod
    def get_reader(cls, extension: str) -> Reader:
        reader = cls._readers.get(extension)
        if reader is None:
            raise ValueError(f"Nenhum leitor encontrado para a extensão '{extension}'")
        return reader

class DataIngestion:
    def read_data(self, path: str) -> Dict[str, pd.DataFrame]:
        dataframes = {}
        files_in_directory = os.listdir(path)
        file_delimiter_mapping = configs.get('file_delimiter_mapping', {})

        for file_name in files_in_directory:
            file_path = os.path.join(path, file_name)
            if os.path.isfile(file_path):
                extension = os.path.splitext(file_name)[1].lower()
                try:
                    reader = ReaderFactory.get_reader(extension)
                    delimiter = file_delimiter_mapping.get(file_name, None)
                    df = reader.read(file_path, delimiter=delimiter)
                    dataframes[file_name] = df
                    logging.info(f"O arquivo '{file_name}' foi carregado com sucesso.")
                except Exception as e:
                    logging.error(f"Erro ao carregar o arquivo '{file_name}': {e}")
        return dataframes