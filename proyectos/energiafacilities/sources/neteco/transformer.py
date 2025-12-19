
import zipfile
import pandas as pd
from pathlib import Path
import shutil
import logging
logger=logging.getLogger(__name__)
def extract_zip(zip_path: str, extract_to: str) -> list[Path]:
    """
    Descomprime un archivo zip en la carpeta `extract_to`.
    Devuelve lista de archivos extraídos.
    """
    extracted_files = []
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
        extracted_files = [Path(extract_to) / name for name in zip_ref.namelist()]
    return extracted_files

def read_excel_sheet(file_path: Path, sheet_name: str = "1 hour") -> pd.DataFrame:
    """
    Lee la hoja especificada de un archivo Excel.
    """
    return pd.read_excel(file_path, sheet_name=sheet_name, skiprows=5)



def combine_excel_from_zip(zip_path: str, sheet_name: str = "1 hour") -> pd.DataFrame:
    # Generamos una carpeta temporal en la misma ruta del zip
    """
    Extrae un archivo zip en una carpeta temporal y lee solo los archivos Excel 
    que contengan la hoja especificada. Luego, combina todos los DataFrames 
    en uno solo y devuelve el resultado.
    """

    temp_folder = Path(zip_path).parent / "extract"

    # Borrar carpeta previa si existe (con todo su contenido)
    if temp_folder.exists():
        shutil.rmtree(temp_folder)
    
    temp_folder.mkdir(exist_ok=True)
    
    # Extraer zip
    files = extract_zip(zip_path, temp_folder)
    
    # Leer solo archivos Excel y combinar
    dfs = []
    for f in files:
        if f.suffix in ['.xlsx', '.xls']:
            try:
                df = read_excel_sheet(f, sheet_name)
                dfs.append(df)
            except Exception as e:
                print(f"No se pudo leer {f}: {e}")
                
    # Concatenar todos los DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

def transformer(zip_path: str) -> str: 
    zip_file = "tmp/neteco/HistoricalData_20251219000000_20251219003000_20251219110351.zip"
    df_final = combine_excel_from_zip(zip_file)
    path_out=Path(zip_path).parent / "transformed/neteco.xlsx"
    df_final.to_excel(path_out, index=False)

    return path_out