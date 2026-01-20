
import os
import zipfile
import pandas as pd
from pathlib import Path
import shutil
import logging
from core.utils import load_config
logger=logging.getLogger(__name__)

def extract_zip(zip_path: str, extract_to: str) -> list[Path]:
    """
    Descomprime un archivo zip en la carpeta `extract_to`.
    Devuelve lista de archivos extraídos.
    """
    logger.info(f"Extrayendo archivo zip: {zip_path} a {extract_to}")
    extracted_files = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            extracted_files = [Path(extract_to) / name for name in zip_ref.namelist()]
        logger.debug(f"Archivos extraídos: {[str(f) for f in extracted_files]}")
    except Exception as e:
        logger.error(f"Error al extraer zip {zip_path}: {e}")
        raise
    return extracted_files

def read_excel_sheet(file_path: Path, sheet_name: str = "1 hour") -> pd.DataFrame:
    """
    Lee la hoja especificada de un archivo Excel.
    """
    logger.info(f"Leyendo hoja '{sheet_name}' del archivo Excel: {file_path}")
    try:
        return pd.read_excel(file_path, sheet_name=sheet_name, skiprows=5)
    except Exception as e:
        logger.error(f"Error al leer Excel {file_path}, hoja '{sheet_name}': {e}")
        raise

def combine_excel_from_zip(zip_path: str, sheet_name: str = "1 hour") -> pd.DataFrame:
    # Generamos una carpeta temporal en la misma ruta del zip
    """
    Extrae un archivo zip en una carpeta temporal y lee solo los archivos Excel 
    que contengan la hoja especificada. Luego, combina todos los DataFrames 
    en uno solo y devuelve el resultado.
    """
    logger.info(f"Combinando Excels desde zip: {zip_path}")
    temp_folder = Path(zip_path).parent / "extract"

    # Borrar carpeta previa si existe (con todo su contenido)
    if temp_folder.exists():
        logger.debug(f"Eliminando carpeta temporal previa: {temp_folder}")
        shutil.rmtree(temp_folder)
    
    temp_folder.mkdir(exist_ok=True)
    logger.debug(f"Carpeta temporal creada: {temp_folder}")
    
    # Extraer zip
    files = extract_zip(zip_path, temp_folder)
    
    # Leer solo archivos Excel y combinar
    dfs = []
    for f in files:
        if f.suffix in ['.xlsx', '.xls']:
            logger.debug(f"Procesando archivo Excel: {f}")
            try:
                df = read_excel_sheet(f, sheet_name)
                dfs.append(df)
            except Exception as e:
                logger.error(f"No se pudo leer {f}: {e}")
                raise
                
    # Concatenar todos los DataFrames
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"DataFrames combinados exitosamente, total filas: {len(combined_df)}")
    else:
        logger.error("No se encontraron archivos Excel válidos para combinar")
        raise ValueError("No se encontraron archivos Excel válidos para combinar")
    return combined_df

#zip_path = "tmp/neteco/HistoricalData_20251219000000_20251219003000_20251219110351.zip"
def transformer_neteco(zip_path: str) -> str: 
    logger.info("Iniciando transformación de datos Neteco")
    try:
        config = load_config()
        general_config = config.get("neteco", {})
        zipfile_path = zip_path or general_config.get('local_dir', '') + '/' + general_config.get('specific_filename', '')
        if not zipfile_path:
            logger.error("No se proporcionó zip_path ni configuración válida")
            raise ValueError("No se proporcionó zip_path ni configuración válida")
        logger.debug(f"Usando archivo zip: {zipfile_path}")
        df_final = combine_excel_from_zip(zipfile_path)
        path_out = general_config.get('local_destination_dir') or str(Path(zipfile_path).parent / "processed/neteco_diario.xlsx")
        
        logger.debug(f"Guardando resultado en: {path_out}")
        
        if not path_out:
            logger.error("No se especificó un directorio de destino para guardar")
            raise ValueError("No se especificó un directorio de destino para guardar")
        try:
            pathcrear=path_out.rsplit('/',1)[0]
            os.makedirs(pathcrear, exist_ok=True)
            logger.debug(f"Directorio {pathcrear} creado/existente.")
        except Exception as e:
            logger.error(f"No se pudo crear el directorio {pathcrear}: {e}")
            raise
            
        df_final.to_excel(path_out, index=False)
        logger.info("Transformación completada exitosamente")
        return path_out
    except Exception as e:
        logger.error(f"Error en transformación Neteco: {e}")
        raise




