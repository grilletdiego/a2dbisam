import pyodbc
from pymongo import MongoClient

class ODBCManager:
    def __init__(self, dsn, user, password, database):
        self.connection_string = f'DSN={dsn};UID={user};PWD={password};DATABASE={database}'
        self.connection = None
        self.connect()

    def get_connection(self):
        if not self.connection:
            self.connect()
        return self.connection

    def connect(self):
        try:
            self.connection = pyodbc.connect(self.connection_string)
            print('Conexión exitosa a ODBC')
        except pyodbc.Error as e:
            print(f'Error al conectar a la base de datos ODBC: {e}')
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            print('Conexión ODBC cerrada')

odbc_manager = ODBCManager(dsn='data', user='', password='', database='')

class MongoManager:
    def __init__(self, uri, db_name):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None
        self.connect()

    def get_collection(self, collection_name):
        if not self.client:
            self.connect()
        return self.db[collection_name]

    def connect(self):
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            print('Conexión exitosa a MongoDB')
        except Exception as e:
            print(f'Error al conectar a MongoDB: {e}')
            raise

    def close(self):
        if self.client:
            self.client.close()
            print('Conexión MongoDB cerrada')
