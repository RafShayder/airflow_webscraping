from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator # type: ignore
# from proyectos.enervision.test import pruebawebscraping  # Módulo no disponible
from proyectos.teleows.GDE import prueba
import sys


sys.path.insert(0, '/opt/airflow/proyectos')

config = {
    'owner': 'shayder',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    saludo="saludo"
    return saludo

def print_bye():
    return 'Bye!'

with DAG('test_prueba2', default_args=config, schedule=timedelta(minutes=1), catchup=False) as dag:
    task1 = PythonOperator(
        task_id='prueba_scraping_GDE',
        python_callable=prueba
    )

    task2 = PythonOperator(
        task_id='print_bye',
        python_callable=print_bye
    )
    # task3 = PythonOperator(
    #     task_id='scraping',
    #     python_callable=pruebawebscraping  # Función no disponible
    # )





    task1 >> task2  # task3 comentado por dependencia no disponible
    