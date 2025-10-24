"""
DAG para ejecutar el scraper de GDE.
Descarga el reporte Console GDE Export diariamente.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import logging

# Agregar proyectos al path
sys.path.insert(0, '/opt/airflow/proyectos')

# Configurar logging
logger = logging.getLogger(__name__)

# Argumentos por defecto del DAG
default_args = {
    'owner': 'scraper-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def run_gde_scraper():
    """
    Ejecuta el scraper de GDE.
    """
    from scraper_integratel.GDE import run_gde
    
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
    description='Scraper para reporte GDE - Ejecuta diariamente',
    schedule_interval='0 6 * * *',  # Ejecutar diariamente a las 6 AM
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

