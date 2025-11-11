from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")

from core.utils import setup_logging
from sources.sftp_energia.stractor import extraersftp_energia_PD, extraersftp_energia_DA
from sources.sftp_energia.loader import load_sftp_energia_PD, load_sftp_energia_DA
from sources.sftp_energia.run_sp import correr_sftp_energia_PD, correr_sftp_energia_DA
from sources.sftp_energia.geterrortable import get_save_errors_PD, get_save_errors_DA

setup_logging("DEBUG")

def procesar_load_sftp_energia_PD(**kwargs):
    ti = kwargs['ti']
    linkdata = ti.xcom_pull(task_ids='extract_sftp_energia_PD')
    return load_sftp_energia_PD(filepath=linkdata)

def procesar_load_sftp_energia_DA(**kwargs):
    ti = kwargs['ti']
    linkdata = ti.xcom_pull(task_ids='extract_sftp_energia_DA')
    return load_sftp_energia_DA(filepath=linkdata)

config = {
    "owner": "SigmaAnalytics",
    "start_date": datetime(2025, 10, 6),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_recibos_sftp_energia",
    default_args=config,
    schedule="0 0 1 * *",
    catchup=False,
    tags=["energiafacilities"],
) as dag:
    # PD
    extract_pd = PythonOperator(
        task_id="extract_sftp_energia_PD",
        python_callable=extraersftp_energia_PD,
    )
    load_pd = PythonOperator(
        task_id="load_sftp_energia_PD",
        python_callable=procesar_load_sftp_energia_PD,
    )
    sp_pd = PythonOperator(
        task_id="sp_transform_sftp_energia_PD",
        python_callable=correr_sftp_energia_PD,
    )
    errors_pd = PythonOperator(
        task_id="get_errors_sftp_energia_PD",
        python_callable=get_save_errors_PD,
    )

    # DA
    extract_da = PythonOperator(
        task_id="extract_sftp_energia_DA",
        python_callable=extraersftp_energia_DA,
    )
    load_da = PythonOperator(
        task_id="load_sftp_energia_DA",
        python_callable=procesar_load_sftp_energia_DA,
    )
    sp_da = PythonOperator(
        task_id="sp_transform_sftp_energia_DA",
        python_callable=correr_sftp_energia_DA,
    )
    errors_da = PythonOperator(
        task_id="get_errors_sftp_energia_DA",
        python_callable=get_save_errors_DA,
    )

    # Dependencias PD
    extract_pd >> load_pd >> sp_pd >> errors_pd

    # Dependencias DA
    extract_da >> load_da >> sp_da >> errors_da
