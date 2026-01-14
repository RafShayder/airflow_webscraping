from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities.core.utils import setup_logging
from energiafacilities.core.helpers import get_xcom_result
from sources.sftp_base_suministros_activos.stractor import extraer_base_suministros_activos
from sources.sftp_base_suministros_activos.loader import load_base_suministros_activos

setup_logging()

def procesar_load_base_suministros_activos(**kwargs):
    """Procesa la carga de datos Base Suministros Activos desde el archivo extraído"""
    ruta = get_xcom_result(kwargs, 'extract_sftp_base_suministros_activos')
    return load_base_suministros_activos(filepath=ruta)

config = {
    "owner": "SigmaAnalytics",
    "start_date": datetime(2025, 12, 1),  # Fecha reciente para permitir ejecución inmediata
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_etl_sftp_base_suministros_activos",
    default_args=config,
    schedule="0 */3 * * *",  # Cada 3 horas (00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00)
    catchup=False,
    tags=["energiafacilities", "sftp", "base_suministros_activos"],
) as dag:
    extract = PythonOperator(
        task_id="extract_sftp_base_suministros_activos",
        python_callable=extraer_base_suministros_activos,
        doc_md="""
        ### Extract SFTP Base Suministros Activos
        
        1. Conecta al servidor SFTP.
        2. Lista archivos en el directorio `/daas/{env}/energy-facilities/base-suministros-activos/input`.
        3. Filtra archivos que contengan 'base_suministros_activos' en el nombre.
        4. Selecciona el archivo más reciente por fecha de modificación.
        5. Descarga el archivo y retorna la ruta local.
        """,
    )
    
    load = PythonOperator(
        task_id="load_sftp_base_suministros_activos",
        python_callable=procesar_load_base_suministros_activos,
        doc_md="""
        ### Load Base Suministros Activos
        
        1. Obtiene la ruta del archivo extraído del task anterior.
        2. Lee el archivo Excel y mapea las columnas usando mapeo flexible.
        3. Valida los datos.
        4. Carga los datos en la tabla `ods.sftp_hm_base_suministros_activos`.
        """,
    )

    extract >> load

