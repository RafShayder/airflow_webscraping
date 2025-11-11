"""
DAG para ejecutar el scraper de GDE (ejecución manual).
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

from energiafacilities import TeleowsSettings, extraer_gde

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


def run_gde_scraper() -> str:
    """
    Construye la configuración desde Airflow y ejecuta la extracción GDE.
    """
    settings = load_settings_from_airflow()
    logger.info("🚀 Iniciando scraper de GDE...")

    try:
        file_path = extraer_gde(settings=settings)
        logger.info("✅ Scraper GDE completado. Archivo: %s", file_path)
        return str(file_path)
    except Exception as exc:
        logger.error("❌ Error en scraper GDE: %s", exc)
        raise


with DAG(
    "dag_gde_teleows",
    default_args=default_args,
    description="Scraper para reporte GDE - Ejecución manual",
    schedule=None,
    catchup=False,
    tags=["scraper", "gde", "integratel", "teleows"],
) as dag:
    scrape_gde = PythonOperator(
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

    scrape_gde
