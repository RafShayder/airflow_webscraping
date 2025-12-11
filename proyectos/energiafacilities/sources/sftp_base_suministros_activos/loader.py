import sys
from pathlib import Path
from typing import Optional, Dict

# Configurar path para imports cuando se ejecuta directamente
current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[2]))  # /.../energiafacilities
sys.path.insert(0, str(current_path.parents[3]))  # /.../proyectos (para compatibilidad con Airflow)

# Intentar importar con path relativo primero, luego absoluto
try:
    from core.helpers import traerjson, normalize_column_name, create_flexible_mapping
except ImportError:
    # Fallback: usar path absoluto (compatible con Airflow)
    from energiafacilities.core.helpers import traerjson, normalize_column_name, create_flexible_mapping

from core.base_loader import BaseLoaderPostgres
from core.utils import load_config
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def load_base_suministros_activos(filepath: Optional[str] = None):
    """
    Carga datos de Base Suministros Activos desde Excel a PostgreSQL.
    
    Args:
        filepath: Ruta del archivo Excel. Si no se proporciona, busca el más reciente en local_dir.
    
    Returns:
        Diccionario con el resultado de la carga
    """
    config = load_config()
    postgres_config = config.get("postgress", {})
    suministros_config = config.get("sftp_base_suministros_activos", {})
    
    Loader = BaseLoaderPostgres(
        config=postgres_config,
        configload=suministros_config
    )
    
    Loader.validar_conexion()
    
    # Cargar mapeo de columnas desde JSON
    columnas_original = traerjson(
        archivo='config/columnas/map_base_suministros_activos.json',
        valor='ods.sftp_hm_base_suministros_activos'
    )
    
    # Determinar ruta del archivo
    if not filepath:
        local_dir = suministros_config.get("local_dir", "tmp/sftp_base_suministros_activos")
        specific_filename = suministros_config.get("specific_filename", "base_suministros_activos")
        
        # Buscar el archivo más reciente que contenga el texto base en el directorio local
        local_dir_path = Path(local_dir)
        if not local_dir_path.exists():
            raise FileNotFoundError(f"No existe el directorio: {local_dir_path}")
        
        # Buscar archivos Excel que contengan el texto base
        archivos_encontrados = [
            f for f in local_dir_path.glob("*.xlsx")
            if specific_filename.upper() in f.name.upper()
        ]
        
        if not archivos_encontrados:
            raise FileNotFoundError(
                f"No se encontraron archivos que contengan '{specific_filename}' en {local_dir_path}"
            )
        
        # Seleccionar el archivo más reciente por fecha de modificación
        archivo_mas_reciente = max(archivos_encontrados, key=lambda f: f.stat().st_mtime)
        filepath = str(archivo_mas_reciente)
        logger.info(f"Archivo seleccionado automáticamente: {archivo_mas_reciente.name}")
    
    # Leer Excel para obtener columnas reales y crear mapeo flexible
    sheet_name = suministros_config.get("sheet_name", 0)
    df_temp = pd.read_excel(filepath, sheet_name=sheet_name, nrows=0)  # Solo leer encabezados
    excel_columns = list(df_temp.columns)

    # Crear mapeo flexible (intenta exacto, luego normalizado)
    columnas = create_flexible_mapping(columnas_original, excel_columns)

    # Verificar datos y cargar (con strictreview=False para permitir columnas faltantes)
    Loader.verificar_datos(
        data=filepath,
        column_mapping=columnas,
        sheet_name=sheet_name,
        strictreview=False  # Permite columnas faltantes (se insertarán como NULL)
    )
    
    carga = Loader.load_data(
        data=filepath,
        column_mapping=columnas,
        sheet_name=sheet_name
    )
    
    return carga


if __name__ == "__main__":
    import sys
    filepath = sys.argv[1] if len(sys.argv) > 1 else None
    load_base_suministros_activos(filepath)

