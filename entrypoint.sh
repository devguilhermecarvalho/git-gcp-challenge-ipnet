#!/bin/bash
set -e

# Iniciar a aplicação Flask usando Gunicorn em segundo plano
gunicorn -b 0.0.0.0:8080 app:app &

# Esperar 3 segundos
sleep 3

# Navegar para o diretório do projeto dbt
cd dbt_transformations

# Executar o dbt run
dbt run

# Manter o contêiner em execução aguardando o processo do Gunicorn
wait -n

# Sair com o status do primeiro processo que terminar
exit $?