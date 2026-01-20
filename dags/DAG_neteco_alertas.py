"""
DAG para generar reportes de alertas NetEco (faltantes y anomalias).
"""

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore

# Asegurar imports de proyecto
sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from sources.reporte_neteco.reporte_anomalias_consumo import run_reporte_anomalias_consumo
from sources.reporte_neteco.reporte_faltantes import run_reporte_neteco_faltantes


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
    "dag_alertas_neteco",
    default_args=DEFAULT_ARGS,
    description="Reportes XLSX de alertas NetEco (faltantes y anomalias).",
    schedule=None,
    catchup=False,
    tags=["neteco", "alertas", "reporte", "xlsx"],
) as dag:
    generate_faltantes_report = PythonOperator(
        task_id="generate_faltantes_report",
        python_callable=run_reporte_neteco_faltantes,
    )
    generate_anomalias_report = PythonOperator(
        task_id="generate_anomalias_report",
        python_callable=run_reporte_anomalias_consumo,
    )
