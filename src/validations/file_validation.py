from typing import Dict
from concurrent.futures import ThreadPoolExecutor

class FileValidation:
    def __init__(self, parallelism_config: Dict[str, int]):
        self.messages = []
        self.max_workers_io = parallelism_config.get('max_workers_io', 5)

    def validate_and_process_files(self, files: Dict[str, bytes]) -> Dict[str, bytes]:
        valid_files = {}
        with ThreadPoolExecutor(max_workers=self.max_workers_io) as executor:
            futures = {executor.submit(self._validate_file, file_name, file_content): file_name for file_name, file_content in files.items()}
            for future in futures:
                file_name = futures[future]
                result = future.result()
                if result:
                    valid_files[file_name] = result
        return valid_files

    def _validate_file(self, file_name, file_content):
        if file_content:
            message = f"Arquivo '{file_name}' validado com sucesso."
            self.messages.append(message)
            return file_content
        else:
            message = f"Arquivo '{file_name}' está vazio e será ignorado."
            self.messages.append(message)
            return None