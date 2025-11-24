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
sys.path.insert(0, "/opt/airflow/proyectos/energiafacilities")
sys.path.insert(0, "/opt/airflow/proyectos")

# Forzar recarga del módulo para evitar caché
import importlib
if 'energiafacilities.sources.autin_checklist.loader' in sys.modules:
    importlib.reload(sys.modules['energiafacilities.sources.autin_checklist.loader'])

from energiafacilities.sources.autin_checklist.loader import (
    load_single_table,
    TABLAS_DYNAMIC_CHECKLIST
)
from energiafacilities.core.utils import load_config, setup_logging

setup_logging("INFO")

logger = logging.getLogger(__name__)

# Verificar que se cargaron todas las tablas
if len(TABLAS_DYNAMIC_CHECKLIST) != 23:
    raise ValueError(
        f"ERROR: Se esperaban 23 tablas pero se encontraron {len(TABLAS_DYNAMIC_CHECKLIST)}. "
        f"Tablas encontradas: {list(TABLAS_DYNAMIC_CHECKLIST.keys())}"
    )

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
        
        logger.info("Buscando archivo en: %s", base_path)
        
        # Buscar archivos que coincidan con el patrón
        # Primero intentar el nombre exacto
        exact_file = base_path / specific_filename
        if exact_file.exists():
            logger.info("Archivo encontrado (exacto): %s", exact_file)
            return str(exact_file)
        
        # Si no existe, buscar el más reciente que contenga "DynamicChecklist" o "SubPM"
        pattern_files = list(base_path.glob("*DynamicChecklist*.xlsx"))
        pattern_files.extend(list(base_path.glob("*SubPM*.xlsx")))
        
        if pattern_files:
            # Ordenar por fecha de modificación (más reciente primero)
            latest_file = max(pattern_files, key=lambda p: p.stat().st_mtime)
            logger.info("Archivo más reciente encontrado: %s", latest_file)
            return str(latest_file)
        
        # NO buscar cualquier .xlsx porque podría tomar el archivo de GDE por error
        # Solo buscar archivos específicos de Dynamic Checklist
        raise FileNotFoundError(
            f"No se encontró ningún archivo Excel de Dynamic Checklist en {base_path}. "
            f"Busca archivos con 'DynamicChecklist' o 'SubPM' en el nombre. "
            f"Ejecuta primero el stractor de Dynamic Checklist o especifica un archivo mediante Variable 'DYNAMIC_CHECKLIST_TEST_FILE'"
        )
        
    except Exception as e:
        logger.error("Error al buscar archivo: %s", e)
        raise


def set_fecha_carga(**kwargs) -> str:
    """
    Establece la fecha de carga para todas las tablas y la retorna vía XCom.
    Esta fecha será compartida por todas las tareas de carga.
    """
    from datetime import datetime
    fecha_carga = datetime.now()
    fecha_carga_str = fecha_carga.isoformat()
    logger.info("Fecha de carga establecida para todas las tablas: %s", fecha_carga_str)
    return fecha_carga_str


def get_filepath(**kwargs) -> str:
    """
    Obtiene la ruta del archivo Excel a procesar.
    Opciones para especificar el archivo (en orden de prioridad):
    1. Variable de Airflow: DYNAMIC_CHECKLIST_TEST_FILE
    2. Buscar el archivo más reciente en el directorio configurado
    """
    file_path = None
    
    # Opción 1: Variable de Airflow (máxima prioridad)
    try:
        file_path = Variable.get("DYNAMIC_CHECKLIST_TEST_FILE", default=None)
        if file_path:
            logger.info("Usando archivo desde Variable: %s", file_path)
            file_path = str(file_path).strip()
    except Exception:
        pass
    
    # Opción 2: Buscar el archivo más reciente
    if not file_path:
        logger.info("Buscando archivo más reciente...")
        file_path = find_latest_dynamic_checklist_file()
    
    # Validar que el archivo existe
    if not Path(file_path).exists():
        raise FileNotFoundError(f"El archivo especificado no existe: {file_path}")
    
    logger.info("Archivo seleccionado: %s", file_path)
    return file_path


def run_load_single_table(tabla_sql: str, nombre_pestana: str, **kwargs) -> dict:
    """
    Ejecuta la carga de una sola tabla de Dynamic Checklist.
    
    Args:
        tabla_sql: Nombre de la tabla SQL destino
        nombre_pestana: Nombre de la pestaña en el Excel
    """
    ti = kwargs.get('ti')
    
    # Obtener filepath de la tarea anterior
    file_path = ti.xcom_pull(task_ids='get_filepath')
    if not file_path:
        raise ValueError("No se recibió filepath. Verifica que la tarea get_filepath se ejecutó correctamente.")
    
    # Obtener fecha_carga de la tarea intermedia
    fecha_carga_str = ti.xcom_pull(task_ids='set_fecha_carga')
    fecha_carga = None
    if fecha_carga_str:
        from datetime import datetime
        fecha_carga = datetime.fromisoformat(fecha_carga_str)
    
    logger.debug("Cargando tabla '%s' desde: %s", tabla_sql, file_path)
    
    try:
        resultado = load_single_table(
            tabla_sql=tabla_sql,
            nombre_pestana=nombre_pestana,
            filepath=file_path,
            fecha_carga=fecha_carga
        )
        
        if resultado.get('status') == 'success':
            logger.info("Tabla '%s' cargada exitosamente: %s", tabla_sql, resultado.get('etl_msg', 'OK'))
        else:
            logger.error("Error al cargar tabla '%s': %s", tabla_sql, resultado.get('etl_msg', 'Error desconocido'))
        
        return resultado
    except Exception as exc:
        logger.error("Error en loader de tabla '%s': %s", tabla_sql, exc)
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
    "dag_dynamic_checklist_test_loader",
    default_args=default_args,
    description="Test del loader de Dynamic Checklist usando archivo existente - Carga paralela por tabla",
    schedule=None,
    catchup=False,
    tags=["test", "dynamic-checklist", "loader", "testing"],
) as dag:
    # Tarea para obtener el archivo
    get_file = PythonOperator(
        task_id="get_filepath",
        python_callable=get_filepath,
        doc_md="""
        ### Obtener Archivo Excel
        
        Obtiene la ruta del archivo Excel a procesar.
        
        **Opciones para especificar el archivo:**
        
        1. **Variable de Airflow** (recomendado para testing):
           - Variable: `DYNAMIC_CHECKLIST_TEST_FILE`
           - Valor: ruta completa al archivo Excel
           - Ejemplo: `/opt/airflow/tmp/teleows/DynamicChecklist_SubPM.xlsx`
        
        2. **Búsqueda automática**:
           - Busca el archivo más reciente en el directorio configurado
           - Prioridad: nombre exacto > archivos con "DynamicChecklist" > cualquier .xlsx
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
    
    # Crear una tarea de carga para cada tabla (23 tareas paralelas: 11 originales + 12 nuevas)
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
            
            1. Obtiene el archivo Excel de la tarea get_filepath.
            2. Procesa la pestaña '{nombre_pestana}'.
            3. Mapea columnas usando columns_map_checklist.json.
            4. Carga datos en la tabla {tabla_sql} en schema 'raw'.
            5. Retorna resultado de la carga.
            """,
        )
        load_tasks.append(load_task)
    
    # Dependencias: get_file -> set_fecha -> todas las tareas de carga (en paralelo)
    get_file >> set_fecha >> load_tasks

