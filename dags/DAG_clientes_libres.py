from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities.core.utils import setup_logging
from energiafacilities.core.helpers import get_xcom_result
from sources.clientes_libres.stractor import extraersftp_clienteslibres
from sources.clientes_libres.transformer import transformer_clienteslibres
from sources.clientes_libres.loader import load_clienteslibres
from sources.clientes_libres.run_sp import correr_sp_clienteslibres

setup_logging("INFO")

def procesar_transform_clientes_libres(**kwargs):
    path_extraido = get_xcom_result(kwargs, 'extract_clientes_libres')
    return transformer_clienteslibres(filepath=path_extraido)

def procesar_load_clientes_libres(**kwargs):
    path_transformado = get_xcom_result(kwargs, 'transform_clientes_libres')
    return load_clienteslibres(filepath=path_transformado)

default_args = {
    "owner": "SigmaAnalytics",
    "start_date": datetime(2025, 12, 1),  # Fecha reciente para permitir ejecución inmediata
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_etl_clientes_libres",
    default_args=default_args,
    schedule="0 */3 * * *",  # Cada 3 horas (00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00)
    catchup=False,
    tags=["energiafacilities"],
) as dag:
    extract = PythonOperator(
        task_id="extract_clientes_libres",
        python_callable=extraersftp_clienteslibres,
    )
    transform = PythonOperator(
        task_id="transform_clientes_libres",
        python_callable=procesar_transform_clientes_libres,
    )
    load = PythonOperator(
        task_id="load_clientes_libres",
        python_callable=procesar_load_clientes_libres,
    )
    sp = PythonOperator(
        task_id="sp_transform_clientes_libres",
        python_callable=correr_sp_clienteslibres,
    )

    extract >> transform >> load >> sp
