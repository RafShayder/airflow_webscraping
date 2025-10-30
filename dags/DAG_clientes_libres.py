from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")

from core.utils import setup_logging
from sources.clientes_libres.stractor import extraersftp_clienteslibres
from sources.clientes_libres.transformer import transformer_clienteslibres
from sources.clientes_libres.loader import load_clienteslibres
from sources.clientes_libres.run_sp import correr_sp_clienteslibres

setup_logging("INFO")

default_args = {
    "owner": "SigmaAnalytics",
    "start_date": datetime(2025, 10, 6),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_etl_clientes_libres",
    default_args=default_args,
    schedule="0 0 1 * *",
    catchup=False,
) as dag:
    extract = PythonOperator(
        task_id="extract_clientes_libres",
        python_callable=extraersftp_clienteslibres,
    )
    transform = PythonOperator(
        task_id="transform_clientes_libres",
        python_callable=transformer_clienteslibres,
    )
    load = PythonOperator(
        task_id="load_clientes_libres",
        python_callable=load_clienteslibres,
    )
    sp = PythonOperator(
        task_id="sp_transform_clientes_libres",
        python_callable=correr_sp_clienteslibres,
    )

    extract >> transform >> load >> sp
