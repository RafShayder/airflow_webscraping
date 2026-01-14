from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities.core.utils import setup_logging
from energiafacilities.core.helpers import get_xcom_result
from sources.base_sitios.stractor import extraer_basedesitios
from sources.base_sitios.loader import loader_basesitios, loader_bitacora_basesitios
from sources.base_sitios.run_sp import correr_sp_basesitios, correr_sp_bitacora

setup_logging()

def procesar_load_base_sitios(**kwargs):
    ruta = get_xcom_result(kwargs, 'extract_base_sitios')
    return loader_basesitios(filepath=ruta)

def procesar_load_bitacora_sitios(**kwargs):
    ruta = get_xcom_result(kwargs, 'extract_base_sitios')
    return loader_bitacora_basesitios(filepath=ruta)

config = {
    "owner": "SigmaAnalytics",
    "start_date": datetime(2025, 12, 1),  # Fecha reciente para permitir ejecución inmediata
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_etl_base_sitios",
    default_args=config,
    schedule="0 */3 * * *",  # Cada 3 horas (00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00)
    catchup=False,
    tags=["energiafacilities"],
) as dag:
    extract = PythonOperator(
        task_id="extract_base_sitios", 
        python_callable=extraer_basedesitios
    )
    load_base = PythonOperator(
        task_id="load_base_sitios",
        python_callable=procesar_load_base_sitios,
    )
    load_bitacora = PythonOperator(
        task_id="load_bitacora_sitios",
        python_callable=procesar_load_bitacora_sitios,
    )
    sp_base = PythonOperator(
        task_id="sp_transform_base_sitios",
        python_callable=correr_sp_basesitios,
    )
    sp_bitacora = PythonOperator(
        task_id="sp_transform_bitacora_sitios",
        python_callable=correr_sp_bitacora,
    )

    extract >> [load_base, load_bitacora]
    load_base >> sp_base
    load_bitacora >> sp_bitacora
