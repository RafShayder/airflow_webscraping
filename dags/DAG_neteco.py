"""
DAG para ejecutar el scraper de NetEco (solo extracción por ahora).
"""

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore

# Asegurar imports de proyecto
sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities.sources.neteco.scraper import scraper_neteco
from energiafacilities.core.utils import setup_logging

setup_logging("INFO")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "SigmaAnalytics",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_neteco_scraper() -> str:
    """
    Ejecuta la extracción NetEco.
    La configuración se carga automáticamente desde Airflow Connection neteco_{env}
    o desde el YAML si no hay Connection.
    """
    try:
        file_path = scraper_neteco()
        return str(file_path)
    except Exception as exc:
        logger.error("Error en scraper NetEco: %s", exc)
        raise


with DAG(
    "dag_neteco",
    default_args=default_args,
    description="Scraper NetEco - solo extracción",
    schedule="0 */3 * * *",
    catchup=False,
    tags=["scraper", "neteco"],
) as dag:
    extract = PythonOperator(
        task_id="scrape_neteco_report",
        python_callable=run_neteco_scraper,
        doc_md="""
        ### Scraper NetEco

        1. Login al portal NetEco.
        2. Navegación a Historical Data.
        3. Aplicación de filtros y exportación.
        4. Descarga del archivo.
        """,
    )
