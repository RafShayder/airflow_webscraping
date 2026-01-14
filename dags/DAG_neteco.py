"""
DAG para ejecutar el ETL completo de NetEco.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore

# Asegurar imports de proyecto
sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities.core.utils import setup_logging
from energiafacilities.core.helpers import get_xcom_result
from sources.neteco.scraper import scraper_neteco
from sources.neteco.transformer import transformer_neteco
from sources.neteco.loader import load_neteco
from sources.neteco.run_sp import correr_sp_neteco

setup_logging()

def run_neteco_scraper() -> str:
    return str(scraper_neteco())


def procesar_transform_neteco(**kwargs):
    path_extraido = get_xcom_result(kwargs, "extract_neteco")
    return transformer_neteco(path_extraido)


def procesar_load_neteco(**kwargs):
    path_transformado = get_xcom_result(kwargs, "transform_neteco")
    return load_neteco(filepath=path_transformado)


default_args = {
    "owner": "SigmaAnalytics",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "dag_neteco",
    default_args=default_args,
    description="ETL NetEco",
    schedule="0 */3 * * *",
    catchup=False,
    tags=["scraper", "neteco"],
) as dag:
    extract = PythonOperator(
        task_id="extract_neteco",
        python_callable=run_neteco_scraper,
    )
    transform = PythonOperator(
        task_id="transform_neteco",
        python_callable=procesar_transform_neteco,
    )
    load = PythonOperator(
        task_id="load_neteco",
        python_callable=procesar_load_neteco,
    )
    sp = PythonOperator(
        task_id="sp_transform_neteco",
        python_callable=correr_sp_neteco,
    )

    extract >> transform >> load >> sp
