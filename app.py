# app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import pymysql
from databaseLib import Databaselib

app = Flask(__name__)

# Configuración de la base de datos
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'nsAY23J??ya2()'
app.config['MYSQL_DB'] = 'bfss'

# Inicializar la conexión a la base de datos con PyMySQL
def get_db_connection():
    return pymysql.connect(
        host=app.config['MYSQL_HOST'],
        user=app.config['MYSQL_USER'],
        password=app.config['MYSQL_PASSWORD'],
        database=app.config['MYSQL_DB']
    )

# Inicializar la clase Databaselib
db_lib = Databaselib(get_db_connection())
CORS(app)

@app.route('/vatimetro', methods=['GET'])
def get_vatimetro():
    result = db_lib.vatimetro()
    return jsonify(result)

@app.route('/rbmspwr', methods=['GET'])
def rbmspwr():
    result = db_lib.rbmspwr()
    return jsonify(result)

@app.route('/modulos', methods=['GET'])
def get_modulos():
    result = db_lib.modulos()
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)

