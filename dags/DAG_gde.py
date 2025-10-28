"""
DAG para ejecutar el scraper de GDE.
Descarga el reporte Console GDE Export diariamente.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from datetime import datetime, timedelta
import sys
import os
import logging

# Agregar proyectos al path
sys.path.insert(0, '/opt/airflow/proyectos')

# Configurar logging
logger = logging.getLogger(__name__)

# Argumentos por defecto del DAG
default_args = {
    'owner': 'adragui',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def _hydrate_teleows_env():
    """
    Prioriza credenciales desde Airflow (Connections o Variables) para el flujo Teleows.
    """
    try:
        conn = BaseHook.get_connection("teleows_portal")
        if conn.login:
            os.environ["USERNAME"] = conn.login
        if conn.password:
            os.environ["PASSWORD"] = conn.password
        for key, env_key in {
            "download_path": "DOWNLOAD_PATH",
            "options_to_select": "OPTIONS_TO_SELECT",
            "date_mode": "DATE_MODE",
            "date_from": "DATE_FROM",
            "date_to": "DATE_TO",
            "max_iframe_attempts": "MAX_IFRAME_ATTEMPTS",
            "max_status_attempts": "MAX_STATUS_ATTEMPTS",
            "gde_output_filename": "GDE_OUTPUT_FILENAME",
            "export_overwrite_files": "EXPORT_OVERWRITE_FILES",
            # nuevo: proxy HTTP/HTTPS para Chrome/Requests
            "proxy": "PROXY",
        }.items():
            value = conn.extra_dejson.get(key)
            if value is not None:
                os.environ[env_key] = str(value)
    except AirflowNotFoundException:
        pass
    except Exception:
        logger.exception("⚠ No se pudo cargar la conexión 'teleows_portal'")

    for var_key, env_key in [
        ("TELEOWS_USERNAME", "USERNAME"),
        ("TELEOWS_PASSWORD", "PASSWORD"),
        ("TELEOWS_DOWNLOAD_PATH", "DOWNLOAD_PATH"),
        ("TELEOWS_OPTIONS_TO_SELECT", "OPTIONS_TO_SELECT"),
        ("TELEOWS_DATE_MODE", "DATE_MODE"),
        ("TELEOWS_DATE_FROM", "DATE_FROM"),
        ("TELEOWS_DATE_TO", "DATE_TO"),
        ("TELEOWS_MAX_IFRAME_ATTEMPTS", "MAX_IFRAME_ATTEMPTS"),
        ("TELEOWS_MAX_STATUS_ATTEMPTS", "MAX_STATUS_ATTEMPTS"),
        ("TELEOWS_OUTPUT_FILENAME", "GDE_OUTPUT_FILENAME"),
        ("TELEOWS_EXPORT_OVERWRITE", "EXPORT_OVERWRITE_FILES"),
        # opcional: variable para proxy si se define como Variable
        ("TELEOWS_PROXY", "PROXY"),
    ]:
        value = Variable.get(var_key, default_var=None)
        if value is not None:
            os.environ[env_key] = str(value)


def run_gde_scraper():
    """
    Ejecuta el scraper de GDE.
    """
    _hydrate_teleows_env()
    from teleows.GDE import run_gde
    
    logger.info("🚀 Iniciando scraper de GDE...")
    
    try:
        file_path = run_gde(headless=True)
        logger.info(f"✅ Scraper GDE completado. Archivo: {file_path}")
        return str(file_path)
    except Exception as e:
        logger.error(f"❌ Error en scraper GDE: {e}")
        raise


# Definir el DAG
with DAG(
    'gde_scraper',
    default_args=default_args,
    description='Scraper para reporte GDE - Ejecución manual',
    schedule=None,  # Solo ejecución manual
    catchup=False,
    tags=['scraper', 'gde', 'integratel'],
) as dag:
    
    # Tarea principal
    scrape_gde = PythonOperator(
        task_id='scrape_gde_report',
        python_callable=run_gde_scraper,
        doc_md="""
        ### Scraper GDE
        
        Esta tarea ejecuta el scraper para descargar el reporte GDE.
        
        **Proceso:**
        1. Login a la plataforma Integratel
        2. Navegación al módulo GDE
        3. Aplicación de filtros (CM, OPM, último mes)
        4. Descarga del archivo Excel
        
        **Output:** Ruta del archivo descargado
        """,
    )
    
    scrape_gde
