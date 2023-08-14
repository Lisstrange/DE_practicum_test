from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

import requests
import psycopg2
import datetime

# constants
SCHEMA = "yandex"
TABLE = "currency_pair"

BASE_URL = 'https://api.exchangerate.host/'
CURRENCY_FROM = "BTC"
CURRENCY_TO = "USD"

PG_HOSTNAME = 'host.docker.internal'
PG_PORT = '5442'
PG_USERNAME = 'postgres'
PG_PSW = 'postgres'

default_args = {
    "owner": "airflow",
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

variables = Variable.set(key="etl_variables",
                         value={"schema": SCHEMA,
                                "table": TABLE,
                                "currency_from": CURRENCY_FROM,
                                "currency_to": CURRENCY_TO,
                                "base_url": BASE_URL},
                         serialize_json=True)

psycorg_conn_args = dict(
    host=PG_HOSTNAME,
    user='postgres',
    password='password',
    dbname='db',
    port=5442)

dag_variables = Variable.get("etl_variables", deserialize_json=True)


## PG HANDLER
class PgHandler:
    def __init__(self, host, port, user, password, database, options):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.options = options
        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):

        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                options=self.options
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

    def insert_row(self, query: str):
        """
        Записывает строку в указанную таблицу
        :param query - sql запрос для записи данных в таблицу
        :return:
        """
        self.cursor.execute(query)
        self.conn.commit()

    def get_cnt_rows(self, table_name: str):
        """
        Находит число строк таблицы и записывает в airflow Variable
        :param table_name:
        :return:
        """
        query = self.cursor.execute(f"SELECT count(*) FROM {table_name}")
        n_rows = self.cursor.fetchall()[0][0]
        return n_rows


pg_client = PgHandler(host=PG_HOSTNAME,
                      port=PG_PORT,
                      user="postgres",
                      password="postgres",
                      database="yandex",
                      options="-c search_path=yandex,public")


def get_data(ti):
    """
    Выгружает через api валютную пару в соответсвии с параметрами DAG (currency_from/currency_to)
    :return:
    """
    currency_from = dag_variables.get('currency_from')
    currency_to = dag_variables.get('currency_to')
    request_path = f"/convert?from={currency_from}&to={currency_to}"
    url = dag_variables.get('base_url') + request_path
    load_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        response = requests.get(url)
    except Exception as e:
        print(f'Error getting data from request: {e}')
        return
    data = response.json()

    data_to_insert = {
        "load_datetime": load_datetime,
        "currency_date": data["date"],
        "currency_from": currency_from,
        "currency_to": currency_to,
        "convert_value": float(data["result"])
    }

    ti.xcom_push(key='push_data', value=data_to_insert)
    return


def insert_data_to_pg(ti):
    data_to_insert = ti.xcom_pull(key='push_data', task_ids=['load_data'])[0]  # TODO убрать костыль
    print(f"data for insert: {data_to_insert}")

    query = f"""
    INSERT INTO {dag_variables.get('table')} 
    (load_datetime, currency_date, currency_from, currency_to, convert_value ) VALUES (
    '{data_to_insert['load_datetime']}','{data_to_insert['currency_date']}', '{data_to_insert['currency_from']}', 
    '{data_to_insert['currency_to']}', '{data_to_insert['convert_value']}'
    )
    """
    print(f"query for insert{query}")
    pg_client.insert_row(query)

    return None


def set_start_cnt_rows():
    cnt_rows = pg_client.get_cnt_rows(dag_variables.get("table"))
    print(f"Start ctn rows = {cnt_rows}")
    Variable.set("start_cnt_rows", cnt_rows)


def check_trunsaction():
    """
    Проверяет корректность поставки данных путем сравнения числа строк в таблице
    :return:
    """
    end_cnt_rows = pg_client.get_cnt_rows(dag_variables.get("table"))
    start_cnt_rows = int(Variable.get("start_cnt_rows"))
    print(f"end cnt rows = {end_cnt_rows}")
    print(f"start cnt rows = {start_cnt_rows}")
    if int(end_cnt_rows) > start_cnt_rows:
        return True
    else:
        return False


def end_script():
    pg_client.close_connection()
    print("Psycorg2 closed")
    print("Task finished")


with DAG(dag_id="yandex-practicum-load-currency", schedule_interval="0 */3 * * *",
         default_args=default_args, tags=["yandex", "ETL"], catchup=False
         ) as dag:
    dag.doc_md = __doc__

    start_bash_task = BashOperator(task_id='start_bash_task',
                                   bash_command="echo 'Start to run the program!'")

    get_start_ctn_rows = PythonOperator(task_id='get_start_ctn_rows',
                                        python_callable=set_start_cnt_rows,
                                        dag=dag,
                                        )

    load_data = PythonOperator(task_id='load_data',
                               python_callable=get_data,
                               dag=dag,
                               )

    insert_data = PythonOperator(task_id='insert_data',
                                 python_callable=insert_data_to_pg,
                                 dag=dag,
                                 )

    insert_process_control = PythonSensor(task_id="trunsaction_control",
                                          poke_interval=60,
                                          timeout=5,
                                          retries=3,
                                          python_callable=check_trunsaction,
                                          soft_fail=True)

    end_task = PythonOperator(task_id='end_task',
                              python_callable=end_script,
                              dag=dag,
                              )
start_bash_task >> get_start_ctn_rows >> load_data >> insert_data >> insert_process_control >> end_task
