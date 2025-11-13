"""
DAG para ejecutar el scraper de GDE (ejecución manual).
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from airflow.sdk import Variable  # type: ignore

# Asegurar imports de proyecto
sys.path.insert(0, "/opt/airflow/proyectos")
sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")

from energiafacilities.sources.autin_gde.stractor import GDEConfig, extraer_gde
from energiafacilities.core import setup_logging, load_overrides_from_airflow
from energiafacilities.sources.autin_gde.loader import load_gde

setup_logging("INFO")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "adragui",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Campos de configuración específicos de GDE
GDE_CONFIG_FIELDS = [
    "username", "password", "download_path", "proxy", "headless",
    "max_iframe_attempts", "max_status_attempts", "options_to_select",
    "date_mode", "date_from", "date_to", "last_n_days",
    "gde_output_filename", "export_overwrite_files",
]


def run_gde_scraper() -> str:
    """
    Construye la configuración desde Airflow y ejecuta la extracción GDE.
    """
    # Obtener entorno desde variable de entorno o Airflow Variable
    env = os.getenv("ENV_MODE") or Variable.get("ENV_MODE", default="dev")

    # Cargar overrides desde Airflow usando función compartida
    overrides = load_overrides_from_airflow(
        fields=GDE_CONFIG_FIELDS,
        conn_id="teleows_portal",
        variable_prefix="TELEOWS_",
    )

    logger.info("🚀 Iniciando scraper de GDE...")
    logger.info("🔍 Entorno: %s", env)
    logger.info("🔍 Overrides aplicados: %s", list(overrides.keys()))

    try:
        # extraer_gde() internamente carga GDEConfig con env y overrides
        file_path = extraer_gde(env=env, overrides=overrides)
        logger.info("✅ Scraper GDE completado. Archivo: %s", file_path)
        return str(file_path)
    except Exception as exc:
        logger.error("❌ Error en scraper GDE: %s", exc)
        raise


def procesar_load_gde(**kwargs):
    """
    Procesa la carga de datos de GDE hacia PostgreSQL.
    Obtiene el filepath del task anterior mediante XCom.
    """
    ti = kwargs['ti']
    linkdata = ti.xcom_pull(task_ids='scrape_gde_report')
    logger.info(f"📁 Archivo recibido desde extract: {linkdata}")
    return load_gde(filepath=linkdata)


with DAG(
    "dag_gde_teleows",
    default_args=default_args,
    description="Scraper y carga de datos GDE - Ejecución manual",
    schedule=None,
    catchup=False,
    tags=["scraper", "gde", "integratel", "teleows"],
) as dag:
    extract = PythonOperator(
        task_id="scrape_gde_report",
        python_callable=run_gde_scraper,
        doc_md="""
        ### Scraper GDE

        1. Login al portal Integratel.
        2. Aplicación de filtros (CM, OPM, último mes).
        3. Exportación del reporte Console GDE.
        4. Descarga y retorna la ruta del archivo.
        """,
    )

    load = PythonOperator(
        task_id="load_gde",
        python_callable=procesar_load_gde,
        doc_md="""
        ### Loader GDE

        1. Obtiene el archivo descargado del task anterior.
        2. Carga los datos desde la pestaña "Export All Custom" hacia PostgreSQL.
        3. Usa el mapeo de columnas desde columns_map.json.
        4. Agrega la columna fechacarga automáticamente.
        """,
    )

    extract >> load
