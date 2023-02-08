from datetime import datetime
import requests

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

postgres = PostgresHook(postgres_conn_id="postgres")


def _connect_load():
    r = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=10')
    result = r.json()
    
    result_as_tuples = [tuple(_.values()) for _ in result]
    
    conn = postgres.get_conn()
    conn.autocommit = True

    with conn.cursor() as cursor:
        cursor.executemany("insert into public.test values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp)", result_as_tuples)
        
with DAG( dag_id = "test", start_date = datetime(2023, 2, 6), schedule_interval = '40 7 0/12 * *', catchup=False
) as dag:
    
    connect_and_load = PythonOperator(
        task_id = "connect_and_load",
        python_callable = _connect_load
    )