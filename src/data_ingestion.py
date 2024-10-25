import pandas as pd
from typing import Dict
from abc import ABC, abstractmethod
import os
import io
from concurrent.futures import ProcessPoolExecutor

class Reader(ABC):
    @abstractmethod
    def read(self, file_content: bytes, delimiter=None) -> pd.DataFrame:
        pass

class CSVReader(Reader):
    def read(self, file_content: bytes, delimiter=None) -> pd.DataFrame:
        try:
            content = file_content.decode('utf-8')
            if delimiter:
                df = pd.read_csv(io.StringIO(content), sep=delimiter, engine='python', header=0)
            else:
                df = pd.read_csv(io.StringIO(content), sep=None, engine='python', header=0)
            return df
        except Exception as e:
            error_message = f"Erro ao ler o conteúdo do arquivo CSV: {e}"
            raise Exception(error_message)

class TSVReader(Reader):
    def read(self, file_content: bytes, delimiter=None) -> pd.DataFrame:
        try:
            content = file_content.decode('utf-8')
            df = pd.read_csv(io.StringIO(content), sep='\t', engine='python', header=0)
            return df
        except Exception as e:
            error_message = f"Erro ao ler o conteúdo do arquivo TSV: {e}"
            raise Exception(error_message)

class ExcelReader(Reader):
    def read(self, file_content: bytes, delimiter=None) -> pd.DataFrame:
        try:
            from io import BytesIO
            df = pd.read_excel(BytesIO(file_content), engine='openpyxl', header=0)
            return df
        except Exception as e:
            error_message = f"Erro ao ler o conteúdo do arquivo Excel: {e}"
            raise Exception(error_message)

class JSONReader(Reader):
    def read(self, file_content: bytes, delimiter=None) -> pd.DataFrame:
        try:
            import json
            data = json.loads(file_content.decode('utf-8'))
            df = pd.json_normalize(data)
            return df
        except Exception as e:
            error_message = f"Erro ao ler o conteúdo do arquivo JSON: {e}"
            raise Exception(error_message)

class ReaderFactory:
    _readers = {
        '.csv': CSVReader(),
        '.tsv': TSVReader(),
        '.txt': CSVReader(),
        '.xlsx': ExcelReader(),
        '.xls': ExcelReader(),
        '.json': JSONReader()
    }

    @classmethod
    def get_reader(cls, extension: str) -> Reader:
        reader = cls._readers.get(extension)
        if reader is None:
            raise ValueError(f"Nenhum leitor encontrado para a extensão '{extension}'")
        return reader

class DataIngestion:
    def __init__(self, file_delimiter_mapping: Dict[str, str], parallelism_config: Dict[str, int]):
        self.file_delimiter_mapping = file_delimiter_mapping
        self.messages = []
        self.max_workers_cpu = parallelism_config.get('max_workers_cpu', 4)

    def read_data(self, files: Dict[str, bytes]) -> Dict[str, pd.DataFrame]:
        dataframes = {}
        with ProcessPoolExecutor(max_workers=self.max_workers_cpu) as executor:
            futures = {executor.submit(self._read_file, file_name, file_content): file_name for file_name, file_content in files.items()}
            for future in futures:
                file_name = futures[future]
                try:
                    df = future.result()
                    dataframes[file_name] = df
                    success_message = f"O arquivo '{file_name}' foi carregado com sucesso."
                    self.messages.append(success_message)
                except Exception as e:
                    error_message = f"Erro ao carregar o arquivo '{file_name}': {e}"
                    self.messages.append(error_message)
        return dataframes

    def _read_file(self, file_name, file_content):
        extension = os.path.splitext(file_name)[1].lower()
        reader = ReaderFactory.get_reader(extension)
        delimiter = self.file_delimiter_mapping.get(file_name)
        df = reader.read(file_content, delimiter=delimiter)
        return df