"""
DAG para ejecutar el scraper de Dynamic Checklist.
Descarga el reporte Dynamic Checklist - Sub PM Query diariamente.
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
    Prioriza credenciales desde Airflow (Connections o Variables).
    Si no existen, conserva los valores que ya estén definidos en el entorno.
    """
    try:
        conn = BaseHook.get_connection("teleows_portal")
        if conn.login:
            os.environ["USERNAME"] = conn.login
        if conn.password:
            os.environ["PASSWORD"] = conn.password
        for key, env_key in {
            "download_path": "DOWNLOAD_PATH",
            "max_iframe_attempts": "MAX_IFRAME_ATTEMPTS",
            "max_status_attempts": "MAX_STATUS_ATTEMPTS",
            "options_to_select": "OPTIONS_TO_SELECT",
            "date_mode": "DATE_MODE",
            "date_from": "DATE_FROM",
            "date_to": "DATE_TO",
            "dynamic_checklist_output_filename": "DYNAMIC_CHECKLIST_OUTPUT_FILENAME",
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
        ("TELEOWS_MAX_IFRAME_ATTEMPTS", "MAX_IFRAME_ATTEMPTS"),
        ("TELEOWS_MAX_STATUS_ATTEMPTS", "MAX_STATUS_ATTEMPTS"),
        ("TELEOWS_OPTIONS_TO_SELECT", "OPTIONS_TO_SELECT"),
        ("TELEOWS_DATE_MODE", "DATE_MODE"),
        ("TELEOWS_DATE_FROM", "DATE_FROM"),
        ("TELEOWS_DATE_TO", "DATE_TO"),
        ("TELEOWS_OUTPUT_FILENAME", "DYNAMIC_CHECKLIST_OUTPUT_FILENAME"),
        ("TELEOWS_EXPORT_OVERWRITE", "EXPORT_OVERWRITE_FILES"),
        # opcional: variable para proxy si se define como Variable
        ("TELEOWS_PROXY", "PROXY"),
    ]:
        value = Variable.get(var_key, default_var=None)
        if value is not None:
            os.environ[env_key] = str(value)


def run_dynamic_checklist_scraper():
    """
    Ejecuta el scraper de Dynamic Checklist.
    """
    _hydrate_teleows_env()
    from teleows.dynamic_checklist import run_dynamic_checklist
    
    logger.info("🚀 Iniciando scraper de Dynamic Checklist...")
    
    try:
        file_path = run_dynamic_checklist(headless=True)
        logger.info(f"✅ Scraper Dynamic Checklist completado. Archivo: {file_path}")
        return str(file_path)
    except Exception as e:
        logger.error(f"❌ Error en scraper Dynamic Checklist: {e}")
        raise


# Definir el DAG
with DAG(
    'dynamic_checklist_scraper',
    default_args=default_args,
    description='Scraper para Dynamic Checklist - Ejecución manual',
    schedule=None,  # Solo ejecución manual
    catchup=False,
    tags=['scraper', 'dynamic-checklist', 'integratel'],
) as dag:
    
    # Tarea principal
    scrape_checklist = PythonOperator(
        task_id='scrape_dynamic_checklist',
        python_callable=run_dynamic_checklist_scraper,
        doc_md="""
        ### Scraper Dynamic Checklist
        
        Esta tarea ejecuta el scraper para descargar el reporte Dynamic Checklist.
        
        **Proceso:**
        1. Login a la plataforma Integratel
        2. Navegación a Dynamic Checklist > Sub PM Query
        3. Aplicación de filtros (último mes)
        4. Descarga del archivo Excel (puede tardar hasta 15 minutos)
        
        **Nota:** Este proceso puede requerir navegación a Log Management si la exportación es en segundo plano.
        """,
    )
    
    scrape_checklist
