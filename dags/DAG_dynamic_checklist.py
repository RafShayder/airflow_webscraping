"""
DAG para ejecutar el scraper de Dynamic Checklist (ejecución manual).
"""

import logging
import sys
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from airflow.sdk.bases.hook import BaseHook  # type: ignore
from airflow.sdk import Variable  # type: ignore

# Asegurar imports de proyecto
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities import TeleowsSettings, extraer_dynamic_checklist
from energiafacilities.sources.dynamic_checklist.loader import load_dynamic_checklist

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

_SETTINGS_FIELDS = {
    "username",
    "password",
    "download_path",
    "max_iframe_attempts",
    "max_status_attempts",
    "options_to_select",
    "date_mode",
    "date_from",
    "date_to",
    "gde_output_filename",
    "dynamic_checklist_output_filename",
    "export_overwrite_files",
    "proxy",
    "headless",
}


def load_settings_from_airflow(
    conn_id: str = "teleows_portal",
    variable_prefix: str = "TELEOWS_",
) -> TeleowsSettings:
    overrides: Dict[str, Any] = {}
    overrides.update(_load_variable_overrides(variable_prefix))
    conn_overrides = _load_connection_overrides(conn_id)
    overrides.update(conn_overrides)

    logger.info(
        "🧩 Generando TeleowsSettings (conn_id=%s, prefix=%s, overrides=%s)",
        conn_id,
        variable_prefix,
        sorted(overrides.keys()),
    )

    return TeleowsSettings.load_with_overrides(overrides)


def _load_connection_overrides(conn_id: str) -> Dict[str, Any]:
    if not conn_id:
        return {}

    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception as exc:
        logger.warning("⚠ No se pudo obtener la conexión '%s': %s", conn_id, exc)
        return {}

    overrides: Dict[str, Any] = {}

    if conn.login:
        overrides["username"] = conn.login
    if conn.password:
        overrides["password"] = conn.password

    extras = getattr(conn, "extra_dejson", {}) or {}
    if isinstance(extras, dict):
        for field in _SETTINGS_FIELDS:
            value = extras.get(field)
            if value is not None:
                overrides[field] = value

    return overrides


def _load_variable_overrides(prefix: str) -> Dict[str, Any]:
    if not prefix:
        return {}

    overrides: Dict[str, Any] = {}

    for field in _SETTINGS_FIELDS:
        var_name = f"{prefix}{field.upper()}"
        try:
            value = Variable.get(var_name)
        except KeyError:
            continue
        except Exception as exc:
            logger.debug("⚠ No se pudo leer la Variable '%s': %s", var_name, exc)
            continue
        overrides[field] = value

    return overrides


def run_dynamic_checklist_scraper() -> str:
    """
    Construye la configuración desde Airflow y ejecuta la extracción Dynamic Checklist.
    """
    settings = load_settings_from_airflow()
    logger.info("🚀 Iniciando scraper de Dynamic Checklist...")

    try:
        file_path = extraer_dynamic_checklist(settings=settings)
        logger.info("✅ Scraper Dynamic Checklist completado. Archivo: %s", file_path)
        return str(file_path)
    except Exception as exc:
        logger.error("❌ Error en scraper Dynamic Checklist: %s", exc)
        raise


def run_dynamic_checklist_loader(**kwargs) -> dict:
    """
    Ejecuta la carga de datos de Dynamic Checklist hacia PostgreSQL.
    Obtiene el filepath del stractor mediante XCom.
    """
    ti = kwargs.get('ti')
    file_path = ti.xcom_pull(task_ids='scrape_dynamic_checklist')
    
    if not file_path:
        raise ValueError("No se recibió filepath del stractor. Verifica que el stractor se ejecutó correctamente.")
    
    logger.info("📥 Iniciando carga de Dynamic Checklist desde: %s", file_path)
    
    try:
        resultado = load_dynamic_checklist(filepath=file_path)
        logger.info("✅ Loader Dynamic Checklist completado: %s", resultado.get('etl_msg', 'OK'))
        return resultado
    except Exception as exc:
        logger.error("❌ Error en loader Dynamic Checklist: %s", exc)
        raise


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

    load_checklist = PythonOperator(
        task_id="load_dynamic_checklist",
        python_callable=run_dynamic_checklist_loader,
        doc_md="""
        ### Loader Dynamic Checklist

        1. Obtiene el archivo Excel del stractor.
        2. Procesa las 11 pestañas del Excel.
        3. Mapea columnas usando columns_map.json.
        4. Carga datos en las tablas correspondientes en schema 'raw'.
        5. Retorna resumen de la carga (tablas exitosas/fallidas).
        """,
    )

    # Dependencias: scrape -> load
    scrape_checklist >> load_checklist
