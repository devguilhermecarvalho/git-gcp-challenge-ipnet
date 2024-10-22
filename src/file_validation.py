import os
import yaml
import glob
import shutil
import logging
import pandas as pd

with open('configs/gcp_config.yaml', 'r') as f:
    configs = yaml.safe_load(f)

bronze_layer_path = configs['bronze_layer_path']
silver_layer_path = configs['silver_layer_path']

class FileValidation:
    def __init__(self):
        self.type_files = ['*.csv', '*.tsv', '*.txt', '*.xlsx', '*.xls']
        self.count_names = {}
        
    def validate_and_process_files(self):
        processed_files = set()

        for file_pattern in self.type_files:
            list_files = glob.glob(os.path.join(bronze_layer_path, file_pattern))
            
            for file_path in list_files:
                file_name = os.path.basename(file_path)
                if file_name in processed_files:
                    continue
                
                processed_files.add(file_name)
                file_size = os.path.getsize(file_path)

                if file_size == 0:
                    logging.error(f'O arquivo {file_name} está vazio.')
                    continue

                validation_file_path = os.path.join(silver_layer_path, file_name)
                if os.path.exists(validation_file_path):
                    base_name, ext = os.path.splitext(file_name)
                    count = self.count_names.get(base_name, 1)
                    new_name = f'{base_name}_{count}{ext}'

                    while os.path.exists(os.path.join(silver_layer_path, new_name)):
                        count += 1
                        new_name = f'{base_name}_{count}{ext}'

                    self.count_names[base_name] = count + 1
                    new_file_path = os.path.join(silver_layer_path, new_name)
                    shutil.move(file_path, new_file_path)
                    logging.info(f'O arquivo {file_name} foi renomeado para {new_name} e movido para o diretório de validação.')
                else:
                    shutil.move(file_path, validation_file_path)
                    logging.info(f'O arquivo {file_name} foi movido para o diretório de validação.')

                if file_name.endswith('.txt'):
                    self.convert_txt_to_csv(validation_file_path)

    def convert_txt_to_csv(self, txt_file_path):
        try:
            df = pd.read_csv(txt_file_path, sep=None, engine='python')
            csv_file_path = os.path.splitext(txt_file_path)[0] + '.csv'
            df.to_csv(csv_file_path, index=False, sep=',')
            os.remove(txt_file_path)
            logging.info(f'Convertido {os.path.basename(txt_file_path)} para o formato CSV.')
        except Exception as e:
            logging.error(f'Falha ao converter {os.path.basename(txt_file_path)} para CSV: {e}')
