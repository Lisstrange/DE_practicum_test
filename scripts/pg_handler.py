import psycopg2

# constants
SCHEMA = "yandex"
TABLE = "currency_pair"

CURRENCY_FROM = "BTC"
CURRENCY_TO = "USD"

PG_HOSTNAME = 'localhost'
PG_PORT = '5442'
PG_USERNAME = 'postgres'
PG_PSW = 'postgres'

import psycopg2

class PgHandler:
    """
    Класс, который помогает использовать
    """
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()
            print("Connected to PostgreSQL database!")
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            print(f"Error executing query: {e}")

    def close_connection(self):
        try:
            self.cursor.close()
            self.conn.close()
            print("Connection to PostgreSQL database closed!")
        except Exception as e:
            print(f"Error closing connection to PostgreSQL database: {e}")
