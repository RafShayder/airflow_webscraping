from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator # type: ignore
from proyectos.enervision.test import pruebawebscraping




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

with DAG('test_prueba', default_args=config, schedule=timedelta(minutes=1), catchup=False, tags=["testing"]) as dag:
    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    task2 = PythonOperator(
        task_id='print_bye',
        python_callable=print_bye
    )
    task3 = PythonOperator(
        task_id='scraping',
        python_callable=pruebawebscraping
    )





    task1 >> [task2, task3]
    