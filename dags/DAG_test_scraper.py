"""
DAG para ejecutar el scraper de test (scraper.py en proyectos/test).
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from airflow.sdk import Variable  # type: ignore
from dotenv import load_dotenv

# Cargar variables del .env antes de cualquier otra cosa
env_path = Path("/opt/airflow/.env")
if env_path.exists():
    load_dotenv(env_path, override=False)
else:
    # Intentar cargar desde la raíz del proyecto
    load_dotenv(override=False)

# Asegurar imports de proyecto
sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")
sys.path.insert(0, "/opt/airflow/proyectos/test")

# Importar el módulo scraper
import scraper

from energiafacilities.core.utils import setup_logging

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


def run_test_scraper() -> str:
    """
    Ejecuta el scraper de test (scraper.py).
    
    Configura las variables de entorno necesarias y ejecuta la función main().
    Usa monkey patching para modificar la ruta de descarga y eliminar el input() bloqueante.
    """
    # Obtener credenciales desde .env, variables de entorno o Airflow Variables
    # El .env ya fue cargado al inicio del módulo
    username = os.getenv("GDE_USER") or Variable.get("GDE_USER", default=None)
    password = os.getenv("GDE_PASS") or Variable.get("GDE_PASS", default=None)
    
    # Configurar variables de entorno si están disponibles
    if username:
        os.environ["GDE_USER"] = username
        scraper.USERNAME = username
    if password:
        os.environ["GDE_PASS"] = password
        scraper.PASSWORD = password
    
    # El proxy está hardcoded en scraper.py (telefonica01.gp.inet:8080)
    logger.info("Proxy configurado en código: %s", scraper.PROXY)
    
    # Configurar ruta de descarga para Airflow
    download_path = Variable.get("TEST_SCRAPER_DOWNLOAD_PATH", default="/opt/airflow/tmp/test_scraper")
    os.makedirs(download_path, exist_ok=True)
    
    # Guardar función original de main y input
    original_main = scraper.main
    original_input = __builtins__['input'] if isinstance(__builtins__, dict) else __builtins__.input
    
    # Crear función input que no bloquee en Airflow
    def noop_input(prompt=""):
        """Reemplazo de input() que no bloquea en Airflow."""
        logger.debug("Se llamó input() con prompt: %s (ignorado en Airflow)", prompt)
        return ""
    
    # Reemplazar input() temporalmente
    if isinstance(__builtins__, dict):
        __builtins__['input'] = noop_input
    else:
        __builtins__.input = noop_input
    
    # Modificar main() para usar nuestra ruta de descarga
    import types
    import inspect
    
    # Obtener el código fuente de main y reemplazar la ruta hardcoded
    source = inspect.getsource(original_main)
    # Reemplazar la ruta hardcoded con nuestra variable
    modified_source = source.replace(
        'r"C:\\Users\\Usuario\\Documents\\GitHub\\scraping"',
        f'r"{download_path}"'
    )
    
    # Compilar y ejecutar la versión modificada
    code = compile(modified_source, '<string>', 'exec')
    namespace = scraper.__dict__.copy()
    # Asegurar que las variables de entorno estén disponibles en el namespace
    namespace['os'] = os
    # El PROXY ya está hardcoded en scraper.py, no necesita pasarse
    exec(code, namespace)
    patched_main = namespace['main']
    
    # Configurar el logger del módulo scraper para que use el logger de Airflow
    scraper.logger = logger
    
    logger.info("Iniciando scraper de test")
    logger.info("Usuario: %s", username or scraper.USERNAME)
    logger.info("Ruta de descarga: %s", download_path)
    logger.info("Opciones a seleccionar: %s", scraper.opciones_a_seleccionar)
    logger.info("Variable de fecha: %s", scraper.var)
    
    try:
        # Ejecutar el scraper modificado
        patched_main()
        logger.info("Scraper de test completado")
        return download_path
    except Exception as exc:
        logger.error("Error en scraper de test: %s", exc, exc_info=True)
        raise
    finally:
        # Restaurar funciones originales
        scraper.main = original_main
        if isinstance(__builtins__, dict):
            __builtins__['input'] = original_input
        else:
            __builtins__.input = original_input


with DAG(
    "dag_test_scraper",
    default_args=default_args,
    description="Scraper de test (scraper.py) - Ejecución manual",
    schedule=None,
    catchup=False,
    tags=["scraper", "test", "integratel", "teleows"],
) as dag:
    scrape_task = PythonOperator(
        task_id="scrape_test_report",
        python_callable=run_test_scraper,
        doc_md="""
        ### Scraper de Test
        
        Ejecuta el script scraper.py ubicado en proyectos/test/scraper.py.
        
        Proceso:
        1. Login al portal Integratel.
        2. Aplicación de filtros según configuración (CM, PM, PLM).
        3. Aplicación de filtros de fecha (automático o último mes).
        4. Exportación del reporte.
        5. Monitoreo del estado de exportación.
        6. Descarga del archivo.
        
        Configuración:
        - Usuario/Contraseña: Variables GDE_USER y GDE_PASS
        - Ruta de descarga: Variable TEST_SCRAPER_DOWNLOAD_PATH (default: /opt/airflow/tmp/test_scraper)
        - Opciones a seleccionar: CM, PM, PLM (configurado en el script)
        - Variable de fecha: 1 (automático) o 2 (último mes)
        """,
    )

