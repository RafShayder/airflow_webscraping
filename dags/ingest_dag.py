"""
DAG para ingestar los reportes descargados a PostgreSQL.
Procesa archivos Excel y los carga en la base de datos.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import logging
from pathlib import Path

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
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


def ingest_gde_report():
    """
    Ingesta el reporte GDE más reciente a PostgreSQL.
    """
    from scraper_integratel.ingest_report import main as ingest_main
    
    logger.info("🚀 Iniciando ingesta de reporte GDE...")
    
    # Buscar el archivo GDE más reciente
    temp_dir = Path("/opt/airflow/proyectos/scraper-integratel/temp")
    gde_files = list(temp_dir.glob("Console_GDE_export*.xlsx"))
    
    if not gde_files:
        logger.warning("⚠️ No se encontraron archivos GDE para ingestar")
        return
    
    # Ordenar por fecha de modificación y tomar el más reciente
    latest_file = max(gde_files, key=lambda p: p.stat().st_mtime)
    
    logger.info(f"📁 Archivo a ingestar: {latest_file}")
    
    try:
        # Ejecutar ingesta
        ingest_main([
            str(latest_file),
            "--schema", "raw",
            "--table", "gde_reports",
            "--mode", "append",
            "--db-host", "postgres",
            "--db-port", "5432",
            "--db-name", "airflow",
            "--db-user", "airflow",
            "--db-password", "airflow",
        ])
        logger.info("✅ Ingesta GDE completada")
    except Exception as e:
        logger.error(f"❌ Error en ingesta GDE: {e}")
        raise


def ingest_dynamic_checklist_report():
    """
    Ingesta el reporte Dynamic Checklist más reciente a PostgreSQL.
    """
    from scraper_integratel.ingest_report import main as ingest_main
    
    logger.info("🚀 Iniciando ingesta de reporte Dynamic Checklist...")
    
    # Buscar el archivo Dynamic Checklist más reciente
    temp_dir = Path("/opt/airflow/proyectos/scraper-integratel/temp")
    dc_files = list(temp_dir.glob("DynamicChecklist*.xlsx"))
    
    if not dc_files:
        logger.warning("⚠️ No se encontraron archivos Dynamic Checklist para ingestar")
        return
    
    # Ordenar por fecha de modificación y tomar el más reciente
    latest_file = max(dc_files, key=lambda p: p.stat().st_mtime)
    
    logger.info(f"📁 Archivo a ingestar: {latest_file}")
    
    try:
        # Ejecutar ingesta
        ingest_main([
            str(latest_file),
            "--schema", "raw",
            "--table", "dynamic_checklist_tasks",
            "--mode", "append",
            "--db-host", "postgres",
            "--db-port", "5432",
            "--db-name", "airflow",
            "--db-user", "airflow",
            "--db-password", "airflow",
        ])
        logger.info("✅ Ingesta Dynamic Checklist completada")
    except Exception as e:
        logger.error(f"❌ Error en ingesta Dynamic Checklist: {e}")
        raise


# Definir el DAG
with DAG(
    'ingest_reports',
    default_args=default_args,
    description='Ingesta de reportes a PostgreSQL',
    schedule_interval='0 9 * * *',  # Ejecutar diariamente a las 9 AM (después de los scrapers)
    catchup=False,
    tags=['ingest', 'postgresql', 'integratel'],
) as dag:
    
    # Tareas de ingesta
    ingest_gde = PythonOperator(
        task_id='ingest_gde_report',
        python_callable=ingest_gde_report,
        doc_md="""
        ### Ingesta GDE Report
        
        Busca el archivo GDE más reciente y lo carga a PostgreSQL.
        
        **Tabla destino:** `raw.gde_reports`
        **Modo:** append (agregar registros)
        """,
    )
    
    ingest_checklist = PythonOperator(
        task_id='ingest_dynamic_checklist_report',
        python_callable=ingest_dynamic_checklist_report,
        doc_md="""
        ### Ingesta Dynamic Checklist Report
        
        Busca el archivo Dynamic Checklist más reciente y lo carga a PostgreSQL.
        
        **Tabla destino:** `raw.dynamic_checklist_tasks`
        **Modo:** append (agregar registros)
        """,
    )
    
    # Las ingestas pueden ejecutarse en paralelo
    [ingest_gde, ingest_checklist]

