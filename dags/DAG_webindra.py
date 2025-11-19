from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")

from energiafacilities.core.utils import setup_logging
from sources.webindra.stractor import stractor_indra
from sources.webindra.loader import load_indra
from sources.webindra.run_sp import correr_sp_webindra
from sources.webindra.geterrortable import get_save_errors_indra

setup_logging("INFO")

def procesar_load_webindra(**kwargs):
    ti = kwargs['ti']
    linkdata = ti.xcom_pull(task_ids='extract_webindra')
    return load_indra(filepath=linkdata)

config = {
    "owner": "SigmaAnalytics",
    "start_date": datetime(2025, 10, 6),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_etl_webindra",
    default_args=config,
    schedule="0 0 1 * *",
    catchup=False,
    tags=["energiafacilities"],
) as dag:
    extract = PythonOperator(
        task_id="extract_webindra",
        python_callable=stractor_indra,
    )
    load = PythonOperator(
        task_id="load_webindra",
        python_callable=procesar_load_webindra,
    )
    sp = PythonOperator(
        task_id="sp_transform_webindra",
        python_callable=correr_sp_webindra,
    )
    errors = PythonOperator(
        task_id="get_errors_webindra",
        python_callable=get_save_errors_indra,
    )

    extract >> load >> sp >> errors