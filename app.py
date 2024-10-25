import os
from flask import Flask, jsonify

from src.config import Config
from src.etl_factory import ETLFactory

app = Flask(__name__)

config = Config()
etl_factory = ETLFactory(config)

@app.route('/', methods=['GET'])
def trigger_etl_endpoint():
    try:
        messages = etl_factory.run_etl()
        return jsonify({'message': 'Processo ETL concluído com sucesso', 'details': messages}), 200
    except Exception as e:
        error_message = f"Erro no processo ETL: {e}"
        return jsonify({'error': error_message}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
