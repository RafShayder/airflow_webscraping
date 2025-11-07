from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")

from core.utils import setup_logging
from sources.base_sitios.stractor import extraer_basedesitios
from sources.base_sitios.loader import loader_basesitios, loader_bitacora_basesitios
from sources.base_sitios.run_sp import correr_sp_basesitios, correr_sp_bitacora

setup_logging("INFO")

def procesar_load_base_sitios(**kwargs):
    ti = kwargs['ti']
    resultado_extract = ti.xcom_pull(task_ids='extract_base_sitios')
    ruta = resultado_extract.get("ruta") if isinstance(resultado_extract, dict) else resultado_extract
    return loader_basesitios(filepath=ruta)

def procesar_load_bitacora_sitios(**kwargs):
    ti = kwargs['ti']
    resultado_extract = ti.xcom_pull(task_ids='extract_base_sitios')
    ruta = resultado_extract.get("ruta") if isinstance(resultado_extract, dict) else resultado_extract
    return loader_bitacora_basesitios(filepath=ruta)

config = {
    "owner": "SigmaAnalytics",
    "start_date": datetime(2025, 10, 6),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_etl_base_sitios",
    default_args=config,
    schedule="0 0 1 * *",
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
