"""
Funciones auxiliares para manipulación de archivos, SFTP, JSON y nombres de archivos.
Estas funciones son utilizadas por los módulos de carga, extracción y transformación.
"""

from __future__ import annotations
import logging
from pathlib import Path
import os
import shutil
import json
import re
from datetime import date, timedelta, datetime
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)


def asegurar_directorio_sftp(sftp, ruta_completa):
    """
    Crea directorios recursivamente en un servidor SFTP remoto si no existen.
    Similar a `mkdir -p` para SFTP.
    
    Args:
        sftp: Cliente SFTP (paramiko.SFTPClient)
        ruta_completa: Ruta completa del directorio a crear (ej: "/daas/dev/folder")
    """
    partes = ruta_completa.strip('/').split('/')
    path_actual = ''
    for parte in partes:
        path_actual += '/' + parte
        try:
            sftp.stat(path_actual)
        except FileNotFoundError:
            logger.debug(f"Creando carpeta: {path_actual}")
            sftp.mkdir(path_actual)


def traerjson(archivo='', valor=None):
    """
    Carga un archivo JSON desde el directorio config/ y retorna un valor específico o todo el contenido.
    
    Args:
        archivo: Ruta relativa al directorio proyecto/energiafacilities/ (ej: 'config/columnas/columns_map.json')
        valor: Clave del JSON a retornar. Si es None, retorna todo el JSON.
    
    Returns:
        Valor específico del JSON si se proporciona 'valor', o todo el JSON si es None.
    """
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / archivo

    with open(config_path, 'r', encoding='utf-8') as file:
        datos = json.load(file)
        if valor:
            return datos[valor]
        else:
            return datos


def borrar_ruta(ruta: str):
    """
    Borra el archivo o carpeta indicada.
    Si se pasa la ruta de un archivo, borra ese archivo.
    Si se pasa la ruta de una carpeta, borra la carpeta completa y su contenido.

    Args:
        ruta: Ruta del archivo o carpeta a borrar
    
    Ejemplo:
        borrar_ruta("tmp/sftp_recibps/indra/archivo.xlsx")  # borra solo el archivo
        borrar_ruta("tmp/sftp_recibps/indra")              # borra toda la carpeta 'indra'
    """
    ruta = os.path.abspath(ruta)

    if not os.path.exists(ruta):
        logger.warning(f"La ruta no existe: {ruta}")
        return

    try:
        if os.path.isfile(ruta):
            os.remove(ruta)
            logger.debug(f"Archivo eliminado: {ruta}")

        elif os.path.isdir(ruta):
            shutil.rmtree(ruta)
            logger.debug(f"Carpeta eliminada con todo su contenido: {ruta}")

        else:
            logger.warning(f"Tipo de ruta desconocido no se eliminó ninguna carpeta temporal: {ruta}")

    except Exception as e:
        logger.warning(f"Error al borrar '{ruta}': {e}")


# Funciones específicas de SFTP energía:
def generar_archivo_especifico(
    lista_archivos: List[Dict[str, str | datetime]],
    basearchivo: Optional[str] = None,
    periodo: Optional[str] = None,
    tipo: Optional[str] = None
) -> Optional[Dict[str, str | datetime]]:
    """
    Retorna el archivo más reciente según:
      - El nombre base (`basearchivo`)
      - El periodo especificado (ej. 202509)
      - La fecha de modificación más reciente

    Si no se pasa periodo, usa el mes anterior al actual.
    Si no se pasa tipo, busca entre todos los tipos.

    Args:
        lista_archivos: Lista de diccionarios con 'nombre' y 'fecha_modificacion'
        basearchivo: Nombre base del archivo a buscar
        periodo: Periodo en formato YYYYMM (ej: "202509")
        tipo: Extensión del archivo sin punto (ej: "xlsx")

    Returns:
        Dict con 'nombre', 'fecha_modificacion' y 'tipo' del archivo más reciente,
        o None si no se encuentra ningún archivo.

    Ejemplo:
        basearchivo = "reporte-consumo-energia-PD"
        periodo = "202509"
        tipo = "xlsx"

    Retorna un dict con:
        {'nombre': 'reporte-consumo-energia-PD-202509v2.xlsx',
         'fecha_modificacion': datetime(...),
         'tipo': 'xlsx'}
    """
    if not lista_archivos:
        logger.warning("Lista de archivos vacía.")
        return None

    # -------------------------------
    # Determinar el periodo (por defecto mes anterior)
    # -------------------------------
    if not periodo:
        hoy = date.today()
        ultimo_dia_mes_anterior = hoy.replace(day=1) - timedelta(days=1)
        periodo = f"{ultimo_dia_mes_anterior.year}{ultimo_dia_mes_anterior.month:02d}"

    # -------------------------------
    # Filtrar por nombre base, periodo y tipo
    # -------------------------------
    archivos_filtrados = []
    for f in lista_archivos:
        nombre = f["nombre"]
        extension = nombre.split(".")[-1].lower()
        if (
            (not basearchivo or nombre.startswith(basearchivo))
            and (periodo in nombre)
            and (not tipo or extension == tipo.lower())
        ):
            f["tipo"] = extension
            archivos_filtrados.append(f)

    if not archivos_filtrados:
        logger.error(f"No se encontraron archivos que coincidan con base='{basearchivo}', periodo='{periodo}'")
        return None

    # -------------------------------
    # Seleccionar el archivo con mayor fecha_modificacion
    # -------------------------------
    archivo_mas_reciente = max(archivos_filtrados, key=lambda x: x["fecha_modificacion"])

    logger.debug(f"Archivo seleccionado: {archivo_mas_reciente['nombre']} (modificado {archivo_mas_reciente['fecha_modificacion']})")

    return archivo_mas_reciente


def archivoespecifico_periodo(
    lista_archivos: List[str],
    basearchivo: Optional[str] = None,
    periodo: Optional[str] = None,
    tipo: Optional[str] = None
):
    """
    Genera el nombre de archivo con formato: basearchivo_PERIODO.tipo
    
    Si no se pasa periodo, genera el periodo anterior al actual (YYYYMM).
    Si se pasa el periodo, lo usa tal cual.
    
    Args:
        lista_archivos: Lista de nombres de archivos disponibles
        basearchivo: Nombre base del archivo
        periodo: Periodo en formato YYYYMM (ej: "202509")
        tipo: Extensión del archivo con punto (ej: ".xlsx")
    
    Returns:
        Nombre del archivo generado
    
    Raises:
        FileNotFoundError: Si el archivo generado no existe en la lista
    """
    # si nos pasan un nombre, generamos el periodo anterior al actual, y si nos pasan el periodo, sería con este periodo
    if not periodo:
        hoy = date.today()
        ultimo_dia_mes_anterior = hoy.replace(day=1) - timedelta(days=1)
        periodo = f"{ultimo_dia_mes_anterior.year}{ultimo_dia_mes_anterior.month:02d}"
    nombre_archivo = f"{basearchivo}_{periodo}{tipo or '.xlsx'}"
    if nombre_archivo not in lista_archivos:
        logger.error(f"No hay archivo a extraer: {nombre_archivo}")
        raise FileNotFoundError(f"No hay archivo a extraer: {nombre_archivo}")
    return nombre_archivo


def archivoespecifico_periodo_CL(
    lista_archivos: List[str],
    basearchivo: Optional[str] = None,
    periodo: Optional[str] = None,
    tipo: Optional[str] = None
):
    """
    Genera el nombre de archivo con formato: basearchivo-PERIODO(e).tipo
    
    Formato del periodo: MMYY (mes y año en dos dígitos, ej: "0225" para febrero 2025)
    Si no se pasa periodo, genera el periodo anterior al actual.
    
    Args:
        lista_archivos: Lista de nombres de archivos disponibles
        basearchivo: Nombre base del archivo
        periodo: Periodo en formato MMYY (ej: "0225")
        tipo: Extensión del archivo con punto (ej: ".xlsx")
    
    Returns:
        Nombre del archivo generado
    
    Raises:
        FileNotFoundError: Si el archivo generado no existe en la lista
    
    Ejemplo:
        formato_periodo(e).xlsx
        ejemplo_1225(e).xlsx
    """
    # si nos pasan un periodo generamos el periodo anterior al actual formato mes-año(año en dos digitos) ejem: 0225, y si nos pasan el periodo, sería con este periodo
    if not periodo:
        hoy = date.today()
        ultimo_dia_mes_anterior = hoy.replace(day=1) - timedelta(days=1)
        # Formato mes-año con año en dos dígitos, p. ej. 0225
        periodo = f"{ultimo_dia_mes_anterior.month:02d}{ultimo_dia_mes_anterior.year % 100:02d}"
    # si se pasó periodo, se usa tal cual
    nombre_archivo = f"{basearchivo}-{periodo}(e){tipo or '.xlsx'}"

    if nombre_archivo not in lista_archivos:
        logger.error(f"No hay archivo a extraer: {nombre_archivo}")
        raise FileNotFoundError(f"No hay archivo a extraer: {nombre_archivo}")
    return nombre_archivo


def crearcarpeta(local_dir: str):
    """
    Crea una carpeta local si no existe.
    
    Args:
        local_dir: Ruta de la carpeta a crear
    
    Raises:
        Exception: Si no se puede crear la carpeta
    """
    try:
        os.makedirs(local_dir, exist_ok=True)
        logger.debug(f"Carpeta creada exitosamente: {local_dir}")
    except FileExistsError:
        logger.debug("La carpeta destino ya existe, no se crea")
    except Exception as e:
        logger.error(f"No se puede crear la carpeta {local_dir}: {e}")
        raise


def default_download_path() -> str:
    """
    Retorna el path de descarga por defecto según el entorno.
    
    Returns:
        - Si está en Airflow: "/opt/airflow/proyectos/energiafacilities/temp"
        - Si está en local: "$HOME/Downloads/scraper_downloads"
    """
    if Path("/opt/airflow").exists():
        return "/opt/airflow/proyectos/energiafacilities/temp"
    return str(Path.home() / "Downloads" / "scraper_downloads")


def get_xcom_result(kwargs: dict, task_id: str, key: Optional[str] = None) -> Optional[str]:
    """
    Extrae resultado de XCom de forma consistente desde un task de Airflow.
    
    Maneja dos casos comunes:
    1. Si el resultado es un dict, extrae la clave especificada (por defecto "ruta")
    2. Si el resultado no es un dict, lo retorna directamente
    
    Args:
        kwargs: Diccionario de kwargs pasado a la función del task (debe contener 'ti')
        task_id: ID del task del cual extraer el resultado XCom
        key: Clave a extraer si el resultado es un dict. Por defecto "ruta".
              Si se especifica, también se intentará usar como key en xcom_pull.
    
    Returns:
        Valor extraído del XCom, o None si no se encuentra
    
    Ejemplo:
        # Caso 1: Resultado es un dict con "ruta"
        resultado = {"ruta": "/path/to/file.xlsx", "status": "success"}
        ruta = get_xcom_result(kwargs, 'extract_task')  # Retorna "/path/to/file.xlsx"
        
        # Caso 2: Resultado es un string directo
        resultado = "/path/to/file.xlsx"
        ruta = get_xcom_result(kwargs, 'extract_task')  # Retorna "/path/to/file.xlsx"
        
        # Caso 3: Usar key específica en xcom_pull
        ruta = get_xcom_result(kwargs, 'validar_archivo', key='ruta_archivo_pago_energia')
    """
    if 'ti' not in kwargs:
        logger.warning(f"get_xcom_result: kwargs no contiene 'ti'. No se puede extraer XCom de '{task_id}'")
        return None
    
    ti = kwargs['ti']
    
    # Si se especifica key, intentar usarla directamente en xcom_pull
    if key:
        try:
            result = ti.xcom_pull(task_ids=task_id, key=key)
            if result is not None:
                return result
        except Exception:
            # Si falla, continuar con el método alternativo
            pass
    
    # Extraer resultado del XCom
    try:
        resultado = ti.xcom_pull(task_ids=task_id)
    except Exception as e:
        logger.warning(f"get_xcom_result: Error al extraer XCom de '{task_id}': {e}")
        return None
    
    if resultado is None:
        return None
    
    # Si es un dict, extraer la clave (por defecto "ruta" si no se especificó key)
    if isinstance(resultado, dict):
        key_to_use = key or "ruta"
        return resultado.get(key_to_use)
    
    # Si no es dict, retornar directamente
    return resultado


def normalize_column_name(s: str) -> str:
    """
    Normaliza nombres de columnas para comparación flexible:
    - pasa a minúsculas
    - quita tildes y ñ
    - convierte espacios y guiones en _
    - quita caracteres especiales
    
    Args:
        s: Nombre de columna a normalizar
    
    Returns:
        Nombre de columna normalizado (solo letras minúsculas, números y guiones bajos)
    
    Ejemplo:
        normalize_column_name("Código Suministro") -> "codigo_suministro"
        normalize_column_name("Fecha-Emisión") -> "fecha_emision"
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
    
    Esta función es útil cuando los nombres de columnas en Excel pueden variar
    ligeramente (tildes, mayúsculas, espacios) pero representan la misma columna.
    
    Args:
        column_mapping: Mapeo original (BD -> Excel esperado)
        excel_columns: Lista de columnas reales en el Excel
    
    Returns:
        Mapeo ajustado con las columnas encontradas (exactas o similares)
    
    Ejemplo:
        column_mapping = {"cod_suministro": "Código Suministro"}
        excel_columns = ["Código Suministro", "Fecha"]
        # Retorna: {"cod_suministro": "Código Suministro"} (coincidencia exacta)
        
        column_mapping = {"cod_suministro": "codigo suministro"}
        excel_columns = ["Código Suministro", "Fecha"]
        # Retorna: {"cod_suministro": "Código Suministro"} (coincidencia normalizada)
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

