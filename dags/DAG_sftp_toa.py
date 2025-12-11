from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities.core.utils import setup_logging
from energiafacilities.core.helpers import get_xcom_result
from sources.sftp_toa.stractor import extraer_toa
from sources.sftp_toa.loader import load_toa
from sources.sftp_toa.run_sp import correr_sp_toa

setup_logging("INFO")

def procesar_load_toa(**kwargs):
    """Procesa la carga de datos TOA desde el archivo extraído"""
    ruta = get_xcom_result(kwargs, 'extract_sftp_toa')
    return load_toa(filepath=ruta)

config = {
    "owner": "SigmaAnalytics",
    "start_date": datetime(2025, 12, 1),  # Fecha reciente para permitir ejecución inmediata
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_etl_sftp_toa",
    default_args=config,
    schedule="0 */3 * * *",  # Cada 3 horas (00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00)
    catchup=False,
    tags=["energiafacilities"],
) as dag:
    extract = PythonOperator(
        task_id="extract_sftp_toa",
        python_callable=extraer_toa,
    )
    load = PythonOperator(
        task_id="load_sftp_toa",
        python_callable=procesar_load_toa,
    )
    sp = PythonOperator(
        task_id="sp_transform_sftp_toa",
        python_callable=correr_sp_toa,
    )

    extract >> load >> sp

