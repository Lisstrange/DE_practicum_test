from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

# from my_functions import my_function1, my_function2

# constants
SCHEMA = "yandex"
TABLE = "currency_pair"

BASE_URL = 'https://api.exchangerate.host/'
CURRENCY_FROM = "BTC"
CURRENCY_TO = "USD"

PG_HOSTNAME = 'localhost'
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

variables = Variable.set(key="currency_load_variables",
                         value={"table_name": TABLE,
                                "rate_base": CURRENCY_FROM,
                                "rate_target": CURRENCY_TO,
                                "connection_name": "pg_connection",
                                "url_base": BASE_URL},
                         serialize_json=True)
dag_variables = Variable.get("currency_load_variables", deserialize_json=True)


def test_vars():
    print("TEST1")
    print(dag_variables) # [2023-08-13, 22:00:32 UTC] {logging_mixin.py:150} INFO - {'table_name': 'currency_pair', 'rate_base': 'BTC', 'rate_target': 'USD', 'connection_name': 'pg_connection', 'url_base': 'https://api.exchangerate.host/'}
    print("TEST2")
    return None


with DAG(dag_id="yandex-load-currency", schedule_interval="*/5 * * * *",
         default_args=default_args, tags=["yandex"], catchup=False
         ) as dag:
    dag.doc_md = __doc__

    start_bash_task = BashOperator(task_id='start_bash_task',
                                   bash_command="echo 'Start to run the program!'")

    task1 = PythonOperator(
        task_id='run_my_function1',
        python_callable=test_vars,
        dag=dag,
    )
    #
    # task2 = PythonOperator(
    #     task_id='run_my_function2',
    #     python_callable=my_function2,
    #     dag=dag,
    # )

    end_bash_task = BashOperator(task_id='end_bash_task',
                                 bash_command="echo 'Finish the program!'")

start_bash_task >> end_bash_task
