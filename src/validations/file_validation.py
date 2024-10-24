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
        # Define os tipos de arquivos que serão processados e um dicionário para contagem de nomes duplicados.
        self.type_files = ['*.csv', '*.tsv', '*.txt', '*.xlsx', '*.xls']
        self.count_names = {}
        
    def validate_and_process_files(self):
        processed_files = set()  # Armazena os nomes de arquivos já processados para evitar repetição.

        # Itera sobre cada tipo de arquivo configurado.
        for file_pattern in self.type_files:
            # Encontra todos os arquivos no caminho da camada bronze que correspondem ao padrão atual.
            list_files = glob.glob(os.path.join(bronze_layer_path, file_pattern))
            
            for file_path in list_files:
                file_name = os.path.basename(file_path)  # Extrai apenas o nome do arquivo.

                # Se o arquivo já foi processado, pula para o próximo.
                if file_name in processed_files:
                    continue

                processed_files.add(file_name)  # Marca o arquivo como processado.
                file_size = os.path.getsize(file_path)  # Verifica o tamanho do arquivo.

                # Se o arquivo estiver vazio, registra um erro no log e continua para o próximo arquivo.
                if file_size == 0:
                    logging.error(f'O arquivo {file_name} está vazio.')
                    continue

                # Define o caminho para onde o arquivo será movido na camada silver.
                validation_file_path = os.path.join(silver_layer_path, file_name)
                
                # Verifica se um arquivo com o mesmo nome já existe na camada silver.
                if os.path.exists(validation_file_path):
                    # Renomeia o arquivo incrementando um contador para evitar sobrescrita.
                    base_name, ext = os.path.splitext(file_name)
                    count = self.count_names.get(base_name, 1)
                    new_name = f'{base_name}_{count}{ext}'

                    # Continua incrementando o nome até encontrar um nome não utilizado.
                    while os.path.exists(os.path.join(silver_layer_path, new_name)):
                        count += 1
                        new_name = f'{base_name}_{count}{ext}'

                    self.count_names[base_name] = count + 1  # Atualiza o contador para o próximo arquivo.
                    new_file_path = os.path.join(silver_layer_path, new_name)
                    
                    # Move o arquivo com o novo nome para a camada silver.
                    shutil.move(file_path, new_file_path)
                    logging.info(f'O arquivo {file_name} foi renomeado para {new_name} e movido para o diretório de validação.')
                else:
                    # Move o arquivo diretamente para a camada silver se não houver conflito de nome.
                    shutil.move(file_path, validation_file_path)
                    logging.info(f'O arquivo {file_name} foi movido para o diretório de validação.')

                # Se o arquivo for .txt, chama a função para convertê-lo em CSV.
                if file_name.endswith('.txt'):
                    self.convert_txt_to_csv(validation_file_path)

    def convert_txt_to_csv(self, txt_file_path):
        try:
            # Lê o arquivo .txt como DataFrame, detectando automaticamente o delimitador.
            df = pd.read_csv(txt_file_path, sep=None, engine='python')
            
            # Cria o caminho para o novo arquivo CSV.
            csv_file_path = os.path.splitext(txt_file_path)[0] + '.csv'
            
            # Salva o DataFrame no formato CSV e remove o arquivo .txt original.
            df.to_csv(csv_file_path, index=False, sep=',')
            os.remove(txt_file_path)
            logging.info(f'Convertido {os.path.basename(txt_file_path)} para o formato CSV.')
        except Exception as e:
            # Registra erros durante a conversão de .txt para .csv no log.
            logging.error(f'Falha ao converter {os.path.basename(txt_file_path)} para CSV: {e}')
