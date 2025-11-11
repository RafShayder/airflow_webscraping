"""
DAG de prueba para ejecutar solo el loader de Dynamic Checklist.
Usa un archivo Excel previamente generado (no ejecuta el stractor).
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from airflow.sdk import Variable  # type: ignore

# Asegurar imports de proyecto
sys.path.insert(0, "/opt/airflow/proyectos")

from energiafacilities.sources.dynamic_checklist.loader import load_dynamic_checklist
from energiafacilities.core.utils import load_config

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


def find_latest_dynamic_checklist_file() -> str:
    """
    Busca el archivo Excel más reciente de Dynamic Checklist en el directorio configurado.
    """
    try:
        # Cargar configuración para obtener el directorio
        config = load_config()
        dynamic_checklist_config = config.get("dynamic_checklist", {})
        local_dir = dynamic_checklist_config.get("local_dir", "tmp/teleows")
        specific_filename = dynamic_checklist_config.get("specific_filename", "DynamicChecklist_SubPM.xlsx")
        
        # Construir ruta completa (relativa a /opt/airflow, no dentro de proyectos/)
        # El stractor guarda en /opt/airflow/tmp/teleows/ directamente
        base_path = Path("/opt/airflow") / local_dir
        
        logger.info(f"🔍 Buscando archivo en: {base_path}")
        
        # Buscar archivos que coincidan con el patrón
        # Primero intentar el nombre exacto
        exact_file = base_path / specific_filename
        if exact_file.exists():
            logger.info(f"✅ Archivo encontrado (exacto): {exact_file}")
            return str(exact_file)
        
        # Si no existe, buscar el más reciente que contenga "DynamicChecklist" o "SubPM"
        pattern_files = list(base_path.glob("*DynamicChecklist*.xlsx"))
        pattern_files.extend(list(base_path.glob("*SubPM*.xlsx")))
        
        if pattern_files:
            # Ordenar por fecha de modificación (más reciente primero)
            latest_file = max(pattern_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"✅ Archivo más reciente encontrado: {latest_file}")
            return str(latest_file)
        
        # Si no se encuentra, buscar cualquier .xlsx y tomar el más reciente
        all_excel = list(base_path.glob("*.xlsx"))
        if all_excel:
            latest_file = max(all_excel, key=lambda p: p.stat().st_mtime)
            logger.warning(f"⚠️  Usando archivo Excel más reciente encontrado: {latest_file}")
            return str(latest_file)
        
        raise FileNotFoundError(
            f"No se encontró ningún archivo Excel en {base_path}. "
            f"Ejecuta primero el stractor o especifica un archivo mediante Variable 'DYNAMIC_CHECKLIST_TEST_FILE'"
        )
        
    except Exception as e:
        logger.error(f"❌ Error al buscar archivo: {e}")
        raise


def run_test_loader(**kwargs) -> dict:
    """
    Ejecuta el loader de Dynamic Checklist usando un archivo existente.
    
    Opciones para especificar el archivo (en orden de prioridad):
    1. Variable de Airflow: DYNAMIC_CHECKLIST_TEST_FILE
    2. Buscar el archivo más reciente en el directorio configurado
    """
    file_path = None
    
    # Opción 1: Variable de Airflow (máxima prioridad)
    try:
        file_path = Variable.get("DYNAMIC_CHECKLIST_TEST_FILE", default_var=None)
        if file_path:
            logger.info(f"📁 Usando archivo desde Variable: {file_path}")
            file_path = str(file_path).strip()
    except Exception:
        pass
    
    # Opción 2: Buscar el archivo más reciente
    if not file_path:
        logger.info("🔍 Buscando archivo más reciente...")
        file_path = find_latest_dynamic_checklist_file()
    
    # Validar que el archivo existe
    if not Path(file_path).exists():
        raise FileNotFoundError(f"El archivo especificado no existe: {file_path}")
    
    logger.info(f"📥 Iniciando carga de Dynamic Checklist desde: {file_path}")
    
    try:
        resultado = load_dynamic_checklist(filepath=file_path)
        logger.info("✅ Loader Dynamic Checklist completado")
        logger.info(f"📊 Resumen: {resultado.get('etl_msg', 'OK')}")
        
        # Log detallado si hay resultados por tabla
        if 'resultados' in resultado:
            logger.info(f"✅ Tablas cargadas exitosamente: {len(resultado['resultados'])}")
            for res in resultado['resultados']:
                logger.info(f"   - {res.get('tabla', 'N/A')}: {res.get('resultado', {}).get('etl_msg', 'OK')}")
        
        if 'errores' in resultado and resultado['errores']:
            logger.warning(f"⚠️  Tablas con errores: {len(resultado['errores'])}")
            for err in resultado['errores']:
                logger.warning(f"   - {err.get('tabla', 'N/A')}: {err.get('error', 'Error desconocido')}")
        
        return resultado
    except Exception as exc:
        logger.error(f"❌ Error en loader Dynamic Checklist: {exc}")
        raise


with DAG(
    "dag_dynamic_checklist_test_loader",
    default_args=default_args,
    description="Test del loader de Dynamic Checklist usando archivo existente",
    schedule=None,
    catchup=False,
    tags=["test", "dynamic-checklist", "loader", "testing"],
) as dag:
    test_loader = PythonOperator(
        task_id="test_load_dynamic_checklist",
        python_callable=run_test_loader,
        doc_md="""
        ### Test Loader Dynamic Checklist

        Ejecuta solo el loader usando un archivo Excel previamente generado.
        
        **Opciones para especificar el archivo:**
        
        1. **Variable de Airflow** (recomendado para testing):
           - Variable: `DYNAMIC_CHECKLIST_TEST_FILE`
           - Valor: ruta completa al archivo Excel
           - Ejemplo: `/opt/airflow/proyectos/energiafacilities/tmp/teleows/DynamicChecklist_SubPM.xlsx`
        
        2. **Búsqueda automática**:
           - Busca el archivo más reciente en el directorio configurado
           - Prioridad: nombre exacto > archivos con "DynamicChecklist" > cualquier .xlsx
        
        **Proceso:**
        1. Localiza el archivo Excel (Variable o búsqueda automática)
        2. Procesa las 11 pestañas del Excel
        3. Mapea columnas usando columns_map.json
        4. Carga datos en las tablas correspondientes en schema 'raw'
        5. Retorna resumen de la carga (tablas exitosas/fallidas)
        
        **Nota:** Este DAG NO ejecuta el stractor, solo el loader.
        """,
    )

    test_loader

