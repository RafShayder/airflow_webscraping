"""
DAG de prueba para demostrar el auto-descubrimiento de Connections.

Este DAG ejecuta la prueba de auto-descubrimiento que está definida en:
proyectos/test/test_autodescubrimiento.py

Auto-descubrimiento:
- Busca Connection: "nombre_seccion_{env}" (ej: "mi_api_test_dev")
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Asegurar imports de proyecto
sys.path.insert(0, "/opt/airflow/proyectos")
sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")

from energiafacilities.core.utils import setup_logging
from test.test_autodescubrimiento import test_autodescubrimiento_connection

setup_logging("INFO")


config = {
    'owner': 'prueba',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'test_autodescubrimiento_connections',
    default_args=config,
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["testing", "connections"]
) as dag:
    
    task1 = PythonOperator(
        task_id='test_connection',
        python_callable=test_autodescubrimiento_connection
    )
    
    task1
