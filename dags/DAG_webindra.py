from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")

from core.utils import setup_logging
from sources.webindra.stractor import stractor_indra
from sources.webindra.loader import load_indra
from sources.webindra.run_sp import correr_sp_webindra
from sources.webindra.geterrortable import get_save_errors

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
    "dag_etl_webindra",
    default_args=default_args,
    schedule="0 0 1 * *",
    catchup=False,
) as dag:
    extract = PythonOperator(
        task_id="extract_webindra",
        python_callable=stractor_indra,
    )
    load = PythonOperator(
        task_id="load_webindra",
        python_callable=load_indra,
    )
    sp = PythonOperator(
        task_id="sp_transform_webindra",
        python_callable=correr_sp_webindra,
    )
    errors = PythonOperator(
        task_id="export_error_table_webindra",
        python_callable=get_save_errors,
        op_args=["webindra_energia"],
    )

    extract >> load >> sp >> errors
