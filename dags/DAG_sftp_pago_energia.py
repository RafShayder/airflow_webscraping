from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities.core.utils import setup_logging
from sources.sftp_pago_energia.stractor import extraersftp_pago_energia
from sources.sftp_pago_energia.loader import load_sftp_base_sitos
from sources.sftp_pago_energia.run_sp import correr_sftp_pago_energia
from sources.sftp_pago_energia.geterrortable import get_save_errors_energia

setup_logging("INFO")

def validar_archivo_sftp_pago_energia(**kwargs):
    """Verifica si la extracción generó un archivo válido y corta el DAG si no existe"""
    ti = kwargs['ti']
    resultado_extract = ti.xcom_pull(task_ids='extract_sftp_pago_energia')
    ruta = resultado_extract.get("ruta") if isinstance(resultado_extract, dict) else resultado_extract

    if not ruta or (isinstance(ruta, str) and not ruta.strip()):
        return False  # ShortCircuitOperator marcará downstream como SKIPPED

    ti.xcom_push(key="ruta_archivo_pago_energia", value=ruta)
    return True


def procesar_load_sftp_pago_energia(**kwargs):
    """Procesa la carga de datos de pago energía desde el archivo extraído"""
    ti = kwargs['ti']
    ruta = ti.xcom_pull(
        task_ids='validar_archivo_sftp_pago_energia',
        key='ruta_archivo_pago_energia'
    )

    if not ruta:
        return None  # Salvaguarda adicional

    return load_sftp_base_sitos(filepath=ruta)

config = {
    "owner": "SigmaAnalytics",
    "start_date": datetime(2025, 10, 6),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_etl_sftp_pago_energia",
    default_args=config,
    schedule="0 0 1 * *",  # Ejecutar el día 1 de cada mes a las 00:00
    catchup=False,
    tags=["energiafacilities"],
) as dag:
    extract = PythonOperator(
        task_id="extract_sftp_pago_energia",
        python_callable=extraersftp_pago_energia,
    )
    validar_archivo = ShortCircuitOperator(
        task_id="validar_archivo_sftp_pago_energia",
        python_callable=validar_archivo_sftp_pago_energia,
    )
    load = PythonOperator(
        task_id="load_sftp_pago_energia",
        python_callable=procesar_load_sftp_pago_energia,
    )
    sp = PythonOperator(
        task_id="sp_transform_sftp_pago_energia",
        python_callable=correr_sftp_pago_energia,
    )
    errors = PythonOperator(
        task_id="get_errors_sftp_pago_energia",
        python_callable=get_save_errors_energia,
    )

    extract >> validar_archivo >> load >> sp >> errors

