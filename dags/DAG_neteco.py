"""
DAG para ejecutar el ETL completo de NetEco.

Parámetros opcionales (Trigger DAG w/ config):
- date_mode: "auto" o "manual" (si no se especifica, usa el valor del config)
- start_time: Fecha inicio en formato "YYYY-MM-DD HH:MM:SS" (solo para modo manual)
- end_time: Fecha fin en formato "YYYY-MM-DD HH:MM:SS" (solo para modo manual)
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore

# Asegurar imports de proyecto
sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities.core.helpers import get_xcom_result
from energiafacilities.core.utils import setup_logging
from sources.neteco.loader import load_neteco
from sources.neteco.run_sp import correr_sp_neteco
from sources.neteco.scraper import scraper_neteco
from sources.neteco.transformer import transformer_neteco

setup_logging()


def run_neteco_scraper(**kwargs) -> str:
    """Ejecuta el scraper con parámetros opcionales del DAG."""
    params = kwargs.get("params", {})

    # Obtener parámetros opcionales (None si están vacíos)
    date_mode = params.get("date_mode") or None
    start_time = params.get("start_time") or None
    end_time = params.get("end_time") or None

    # Solo pasar valores si fueron especificados
    if date_mode or start_time or end_time:
        return str(
            scraper_neteco(
                date_mode_override=date_mode,
                start_time_override=start_time,
                end_time_override=end_time,
            )
        )
    return str(scraper_neteco())


def procesar_transform_neteco(**kwargs):
    path_extraido = get_xcom_result(kwargs, "extract_neteco")
    return transformer_neteco(path_extraido)


def procesar_load_neteco(**kwargs):
    path_transformado = get_xcom_result(kwargs, "transform_neteco")
    return load_neteco(filepath=path_transformado)


default_args = {
    "owner": "SigmaAnalytics",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Parámetros opcionales para ejecución manual
dag_params = {
    "date_mode": Param(
        default=None,
        type=["null", "string"],
        title="Modo de fecha (opcional)",
        description="Vacío = usar config. 'auto' = día anterior. 'manual' = requiere fechas abajo.",
    ),
    "start_time": Param(
        default=None,
        type=["null", "string"],
        title="Fecha inicio (solo si date_mode=manual)",
        description="Formato: YYYY-MM-DD HH:MM:SS (ej: 2026-01-15 00:00:01). Dejar vacío si date_mode=auto.",
    ),
    "end_time": Param(
        default=None,
        type=["null", "string"],
        title="Fecha fin (solo si date_mode=manual)",
        description="Formato: YYYY-MM-DD HH:MM:SS (ej: 2026-01-15 23:59:59). Máximo 31 días. Dejar vacío si date_mode=auto.",
    ),
}

with DAG(
    "dag_neteco",
    default_args=default_args,
    description="ETL NetEco",
    schedule="0 5 * * *",  # Cada día a las 5 am
    catchup=False,
    tags=["scraper", "neteco"],
    params=dag_params,
    render_template_as_native_obj=True,
) as dag:
    extract = PythonOperator(
        task_id="extract_neteco",
        python_callable=run_neteco_scraper,
    )
    transform = PythonOperator(
        task_id="transform_neteco",
        python_callable=procesar_transform_neteco,
    )
    load = PythonOperator(
        task_id="load_neteco",
        python_callable=procesar_load_neteco,
    )
    sp = PythonOperator(
        task_id="sp_transform_neteco",
        python_callable=correr_sp_neteco,
    )

    extract >> transform >> load >> sp
