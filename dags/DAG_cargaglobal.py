from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Asegura que Airflow encuentre los módulos del proyecto dentro del contenedor
sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities.core.utils import setup_logging
from energiafacilities.core.base_run_sp import run_sp
from sources.cargaglobal.cargamanual import load_manual

setup_logging()


def ejecutar(**kwargs):
    params = kwargs.get("params", {})
    filepath = params.get("filepath")
    # Valores recibidos desde el Trigger (prellenados con defaults)
    modo = params.get('modo')
    schema = params.get('schema')
    table = params.get('table')
    sp = params.get('sp')
    carga = load_manual(
        filepath=filepath, schema=schema, table_name=table, modo=modo
    )
    if sp:
        run_sp(configyaml="", sp_value=sp)
    return carga


default_args = {
    "owner": "SigmaAnalytics",
    "start_date": datetime(2025, 10, 6),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

params = {
    'filepath': "",  # requerido al disparar
    'modo': 'replace',
    'schema': 'raw',
    'table': 'excel_hm_consumo_energia',
    'sp': "",  # opcional
}

with DAG(
    "dag_cargaglobal",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["energiafacilities", "cargaglobal"],
    params=params,
) as dag:
    PythonOperator(
        task_id="run",
        python_callable=ejecutar,
    )

