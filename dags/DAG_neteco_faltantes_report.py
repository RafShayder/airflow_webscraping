"""
DAG para generar un reporte XLSX de faltantes de data por sitio (NetEco).
"""

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore

# Asegurar imports de proyecto
sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from sources.reporte_neteco.reporte_python import run_reporte_neteco_faltantes_python


logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "SigmaAnalytics",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "dag_neteco_faltantes_report",
    default_args=DEFAULT_ARGS,
    description="Reporte XLSX de faltantes NetEco (mes actual).",
    schedule=None,
    catchup=False,
    tags=["neteco", "reporte", "xlsx"],
) as dag:
    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=run_reporte_neteco_faltantes_python,
    )
