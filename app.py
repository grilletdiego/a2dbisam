from flask import Flask
from flask_cors import CORS
from db_manager import odbc_manager, MongoManager 

from OperacionesInventario import operaciones_bp

def create_app():
    app = Flask(__name__)
    CORS(app)

    # Configurar gestores de base de datos

    app.odbc_manager = odbc_manager
    app.mongo_manager = MongoManager(uri='mongodb://localhost:27017/', db_name='miBaseDeDatos')
   

    # Registrar blueprints y otras partes de la aplicación aquí
    # app.register_blueprint(your_blueprint)
    # Registrar los Blueprints
    
    app.register_blueprint(operaciones_bp)

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=3000, debug=True)