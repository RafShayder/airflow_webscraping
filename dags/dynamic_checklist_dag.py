"""
DAG para ejecutar el scraper de Dynamic Checklist.
Descarga el reporte Dynamic Checklist - Sub PM Query diariamente.
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


def run_dynamic_checklist_scraper():
    """
    Ejecuta el scraper de Dynamic Checklist.
    """
    from scraper_integratel.dynamic_checklist import run_dynamic_checklist
    
    logger.info("🚀 Iniciando scraper de Dynamic Checklist...")
    
    try:
        run_dynamic_checklist(headless=True)
        logger.info("✅ Scraper Dynamic Checklist completado")
    except Exception as e:
        logger.error(f"❌ Error en scraper Dynamic Checklist: {e}")
        raise


# Definir el DAG
with DAG(
    'dynamic_checklist_scraper',
    default_args=default_args,
    description='Scraper para Dynamic Checklist - Ejecuta diariamente',
    schedule_interval='0 7 * * *',  # Ejecutar diariamente a las 7 AM
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

