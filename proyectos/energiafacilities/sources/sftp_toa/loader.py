import sys
import re
from pathlib import Path
from typing import Optional, Dict

# Configurar path para imports cuando se ejecuta directamente
current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[2]))  # /.../energiafacilities

from core.base_loader import BaseLoaderPostgres
from core.utils import load_config
from core.helpers import traerjson
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def normalize_column_name(s: str) -> str:
    """
    Normaliza nombres de columnas para comparación flexible:
    - pasa a minúsculas
    - quita tildes y ñ
    - convierte espacios y guiones en _
    - quita caracteres especiales
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = (s.replace("á", "a").replace("é", "e").replace("í", "i")
           .replace("ó", "o").replace("ú", "u").replace("ñ", "n"))
    # espacios y guiones -> _
    s = re.sub(r"[-\s]+", "_", s)
    # solo letras, números y _
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s


def create_flexible_mapping(column_mapping: Dict[str, str], excel_columns: list) -> Dict[str, str]:
    """
    Crea un mapeo flexible que intenta primero coincidencia exacta,
    y si no encuentra, busca columnas similares (normalizadas).
    
    Args:
        column_mapping: Mapeo original (BD -> Excel esperado)
        excel_columns: Lista de columnas reales en el Excel
    
    Returns:
        Mapeo ajustado con las columnas encontradas (exactas o similares)
    """
    flexible_mapping = {}
    excel_cols_normalized = {normalize_column_name(col): col for col in excel_columns}
    
    for bd_col, excel_expected in column_mapping.items():
        # 1. Intentar coincidencia exacta
        if excel_expected in excel_columns:
            flexible_mapping[bd_col] = excel_expected
            logger.debug(f"Mapeo exacto: {bd_col} -> {excel_expected}")
        else:
            # 2. Intentar coincidencia normalizada
            expected_normalized = normalize_column_name(excel_expected)
            if expected_normalized in excel_cols_normalized:
                excel_found = excel_cols_normalized[expected_normalized]
                flexible_mapping[bd_col] = excel_found
                logger.debug(f"Mapeo flexible encontrado: {bd_col} -> '{excel_found}' (esperado: '{excel_expected}')")
            else:
                # 3. No se encontró, se insertará como NULL
                logger.debug(f"No se encontró columna para '{bd_col}' (esperado: '{excel_expected}'). Se insertará como NULL.")
                # No agregamos al mapeo, BaseLoaderPostgres manejará las columnas faltantes
    
    return flexible_mapping


def load_toa(filepath: Optional[str] = None):
    """
    Carga datos de TOA desde Excel a PostgreSQL.
    
    Args:
        filepath: Ruta del archivo Excel. Si no se proporciona, busca el más reciente en local_dir.
    
    Returns:
        Diccionario con el resultado de la carga
    """
    config = load_config()
    postgres_config = config.get("postgress", {})
    toa_config = config.get("sftp_toa", {})
    
    Loader = BaseLoaderPostgres(
        config=postgres_config,
        configload=toa_config
    )
    
    Loader.validar_conexion()
    
    # Cargar mapeo de columnas desde maptoa.json
    columnas_original = traerjson(archivo='config/columnas/maptoa.json', valor='raw.sftp_hd_toa')
    
    # Determinar ruta del archivo
    if not filepath:
        local_dir = toa_config.get("local_dir", "tmp/sftp_toa")
        specific_filename = toa_config.get("specific_filename", "TOA")
        
        # Buscar el archivo más reciente que contenga "TOA" en el directorio local
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
    sheet_name = toa_config.get("sheet_name", 0)
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
    load_toa(filepath)
