"""
DAG para ejecutar el scraper de Dynamic Checklist (ejecución manual).
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

from energiafacilities import DynamicChecklistConfig, extraer_dynamic_checklist
from energiafacilities.core import setup_logging, load_overrides_from_airflow
from energiafacilities.sources.autin_checklist.loader import (
    load_dynamic_checklist,
    load_single_table,
    TABLAS_DYNAMIC_CHECKLIST
)

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

# Campos de configuración específicos de Dynamic Checklist
DYNAMIC_CHECKLIST_CONFIG_FIELDS = [
    "username", "password", "download_path", "proxy", "headless",
    "max_iframe_attempts", "max_status_attempts",
    "date_mode", "date_from", "date_to", "last_n_days",
    "dynamic_checklist_output_filename", "export_overwrite_files",
]


def run_dynamic_checklist_scraper() -> str:
    """
    Construye la configuración desde Airflow y ejecuta la extracción Dynamic Checklist.
    """
    # Obtener entorno desde variable de entorno o Airflow Variable
    env = os.getenv("ENV_MODE") or Variable.get("ENV_MODE", default_var="dev")

    # Cargar overrides desde Airflow usando función compartida
    overrides = load_overrides_from_airflow(
        fields=DYNAMIC_CHECKLIST_CONFIG_FIELDS,
        conn_id="teleows_portal",
        variable_prefix="TELEOWS_",
    )

    logger.info("🚀 Iniciando scraper de Dynamic Checklist...")
    logger.info("🔍 Entorno: %s", env)
    logger.info("🔍 Overrides aplicados: %s", list(overrides.keys()))

    try:
        # extraer_dynamic_checklist() internamente carga DynamicChecklistConfig con env y overrides
        file_path = extraer_dynamic_checklist(env=env, overrides=overrides)
        logger.info("✅ Scraper Dynamic Checklist completado. Archivo: %s", file_path)
        return str(file_path)
    except Exception as exc:
        logger.error("❌ Error en scraper Dynamic Checklist: %s", exc)
        raise


def set_fecha_carga(**kwargs) -> str:
    """
    Establece la fecha de carga para todas las tablas y la retorna vía XCom.
    Esta fecha será compartida por todas las tareas de carga.
    """
    from datetime import datetime
    fecha_carga = datetime.now()
    fecha_carga_str = fecha_carga.isoformat()
    logger.info("📅 Fecha de carga establecida para todas las tablas: %s", fecha_carga_str)
    return fecha_carga_str


def run_load_single_table(tabla_sql: str, nombre_pestana: str, **kwargs) -> dict:
    """
    Ejecuta la carga de una sola tabla de Dynamic Checklist.
    
    Args:
        tabla_sql: Nombre de la tabla SQL destino
        nombre_pestana: Nombre de la pestaña en el Excel
    """
    ti = kwargs.get('ti')
    
    # Obtener filepath del scraper
    file_path = ti.xcom_pull(task_ids='scrape_dynamic_checklist')
    if not file_path:
        raise ValueError("No se recibió filepath del stractor. Verifica que el stractor se ejecutó correctamente.")
    
    # Obtener fecha_carga de la tarea intermedia
    fecha_carga_str = ti.xcom_pull(task_ids='set_fecha_carga')
    fecha_carga = None
    if fecha_carga_str:
        from datetime import datetime
        fecha_carga = datetime.fromisoformat(fecha_carga_str)
    
    logger.info("📥 Cargando tabla '%s' desde: %s", tabla_sql, file_path)
    
    try:
        resultado = load_single_table(
            tabla_sql=tabla_sql,
            nombre_pestana=nombre_pestana,
            filepath=file_path,
            fecha_carga=fecha_carga
        )
        
        if resultado.get('status') == 'success':
            logger.info("✅ Tabla '%s' cargada exitosamente: %s", tabla_sql, resultado.get('etl_msg', 'OK'))
        else:
            logger.error("❌ Error al cargar tabla '%s': %s", tabla_sql, resultado.get('etl_msg', 'Error desconocido'))
        
        return resultado
    except Exception as exc:
        logger.error("❌ Error en loader de tabla '%s': %s", tabla_sql, exc)
        raise


def make_table_loader(tabla_sql: str, nombre_pestana: str):
    """
    Crea una función wrapper para cargar una tabla específica.
    Esto evita problemas de closure en el loop.
    """
    def load_table(**kwargs):
        return run_load_single_table(tabla_sql=tabla_sql, nombre_pestana=nombre_pestana, **kwargs)
    return load_table


with DAG(
    "dag_dynamic_checklist_teleows",
    default_args=default_args,
    description="ETL completo para Dynamic Checklist - Ejecución manual",
    schedule=None,
    catchup=False,
    tags=["scraper", "dynamic-checklist", "integratel", "teleows", "etl"],
) as dag:
    scrape_checklist = PythonOperator(
        task_id="scrape_dynamic_checklist",
        python_callable=run_dynamic_checklist_scraper,
        doc_md="""
        ### Scraper Dynamic Checklist

        1. Login al portal Integratel.
        2. Navegación a Dynamic checklist > Sub PM Query.
        3. Aplicación de filtros y disparo de la exportación.
        4. Descarga y retorna la ruta del archivo generado (directo o vía Log Management).
        """,
    )

    # Tarea intermedia para establecer fecha_carga compartida
    set_fecha = PythonOperator(
        task_id="set_fecha_carga",
        python_callable=set_fecha_carga,
        doc_md="""
        ### Establecer Fecha de Carga
        
        Establece la fecha y hora de inicio del proceso de carga.
        Esta fecha será compartida por todas las tablas para mantener consistencia.
        """,
    )
    
    # Crear una tarea de carga para cada tabla (11 tareas paralelas)
    load_tasks = []
    for tabla_sql, nombre_pestana in TABLAS_DYNAMIC_CHECKLIST.items():
        # Crear task_id único para cada tabla
        task_id = f"load_table_{tabla_sql}"
        
        # Crear función wrapper con los parámetros fijados (evita problemas de closure)
        load_function = make_table_loader(tabla_sql, nombre_pestana)
        
        load_task = PythonOperator(
            task_id=task_id,
            python_callable=load_function,
            doc_md=f"""
            ### Loader Tabla: {tabla_sql}
            
            1. Obtiene el archivo Excel del stractor.
            2. Procesa la pestaña '{nombre_pestana}'.
            3. Mapea columnas usando columns_map.json.
            4. Carga datos en la tabla {tabla_sql} en schema 'raw'.
            5. Retorna resultado de la carga.
            """,
        )
        load_tasks.append(load_task)
    
    # Dependencias: scrape -> set_fecha -> todas las tareas de carga (en paralelo)
    scrape_checklist >> set_fecha >> load_tasks
