import sys 
sys.path.append("/opt/airflow/proyectos/enervision")

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator # type: ignore
# from proyectos.enervision.sources.sftp_energia.extractor import extraersftp_energia  # Módulo no disponible
# from proyectos.enervision.sources.sftp_energia.loader import leaderftp_energia  # Módulo no disponible


config = {
    'owner': 'shayder',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extraerdatos():
    # mensaje = extraersftp_energia()  # Función no disponible
    mensaje = "Función no disponible - módulo enervision no encontrado"
    return mensaje

def loaderdatos(**context):
    # rutapath = context['ti'].xcom_pull(task_ids='sftp_recibos_extraer_datos')
    # print("ruta completa existe  ? : ", rutapath)
    # print("alerta: ",rutapath['ruta'])
    # mensaje = leaderftp_energia(rutapath['ruta'])  # Función no disponible
    mensaje = "Función no disponible - módulo enervision no encontrado"
    return mensaje

with DAG('dag_recibos_sftp_energia', default_args=config, schedule=timedelta(minutes=1), catchup=False) as dag:
    task1 = PythonOperator(
        task_id='sftp_recibos_extraer_datos',
        python_callable=extraerdatos
    )
    
    task2 =PythonOperator(
        task_id='sftp_recibos_loead_datos',
        python_callable=loaderdatos
    )



    task1 >> task2 # >> [task2, task3]
    