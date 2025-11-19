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

