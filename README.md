# Desafio - ETL utilizando as ferramentas do Google Cloud

Este projeto é um desafio que implementa um pipeline ETL (Extract, Transform, Load) que lê arquivos de um bucket no Google Cloud Storage, processa e valida os dados, e os carrega em tabelas do BigQuery. Após o carregamento, o dbt é utilizado para transformar os dados no BigQuery. O projeto está preparado para ser implantado no Cloud Run utilizando o Cloud Build.

A arquitetura do projeto foi desenvolvida seguindo os princípios do **SOLID** e boas práticas de engenharia de software para garantir manutenção, reusabilidade e escalabilidade.

O pipeline ETL é composto pelas seguintes etapas:

1. **Extração** : Baixa arquivos do Cloud Storage (camada bronze).
2. **Validação de Arquivos** : Verifica se os arquivos estão corretos e não estão corrompidos.
3. **Ingestão de Dados**: Lê os arquivos e os converte em DataFrames do Pandas.
4. **Validação de Dados**: Verifica a consistência e qualidade dos dados.
5. **Carga no BigQuery**: Carrega os dados validados em tabelas do BigQuery.
6. **Transformações com dbt**: Executa modelos dbt para transformar os dados no BigQuery.
7. **Upload para a Camada Silver**: Envia os arquivos processados para a camada silver no Cloud Storage.

## Tecnologias Utilizadas

* **Python 3.9**
* **Flask** : Framework web para criar a API.
* **Gunicorn** : Servidor WSGI para executar a aplicação Flask em produção.
* **Pandas** : Biblioteca para manipulação e análise de dados.
* **Google Cloud Storage Client Library**
* **Google Cloud BigQuery Client Library**
* **dbt (Data Build Tool)**: Ferramenta para transformação de dados em SQL no BigQuery.
* **Docker** : Para containerização da aplicação.
* **Google Cloud Run**: Plataforma para executar contêineres sem servidor.
* **Google Cloud Build**: Serviço para implantação contínua.
* **YAML** : Utilizado para arquivos de configuração.
* **Concurrent Futures** : Biblioteca para implementar paralelismo e assincronismo.
* **Git** : Controle de versão.
* **Google Cloud SDK (gcloud)** : Ferramenta de linha de comando para interagir com os serviços do GCP.

## Contato

Para mais informações, entre em contato:

**Name:** Guilherme Carvalho

**E-mail:** guilhermerdcarvalho@gmail.com

**LinkedIn:** [https://www.linkedin.com/in/devguilhermecarvalho/](https://www.linkedin.com/in/devguilhermecarvalho/)
