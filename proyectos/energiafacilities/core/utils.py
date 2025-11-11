from __future__ import annotations
import logging
from envyaml import EnvYAML
from pathlib import Path
from dotenv import load_dotenv
import os
import shutil
import json
from datetime import date, timedelta, datetime
from typing import List, Optional, Dict, Any
import sys

logger = logging.getLogger(__name__)

 # Funciones globales 
def setup_logging(level: str = "DEBUG") -> None:
    handler = logging.StreamHandler(sys.stdout)  # 
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    handler.setFormatter(formatter)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=[handler]
    ) 

def load_config(env: str | None = None) -> dict:
    """
    Carga un archivo YAML con soporte automático para variables de entorno .
    Ejemplo de uso en el YAML:
        postgres:
          user: ${POSTGRES_USER}
          password: ${POSTGRES_PASS}

    Si las variables existen en el entorno, se reemplazan automáticamente.

    """
    try:
        # Determinar directorio base de energiafacilities
        base_dir = Path(__file__).resolve().parent.parent

        # Cargar variables del .env de energiafacilities si existe
        dotenv_path = base_dir / ".env"
        if dotenv_path.exists():
            logger.debug(f"Cargando variables de entorno desde: {dotenv_path}")
            load_dotenv(dotenv_path=dotenv_path, override=False)
        else:
            logger.debug("No se encontró archivo .env en energiafacilities, usando variables del sistema")
            load_dotenv()  # Buscar en directorio actual o padres

        env = env or os.getenv("ENV_MODE", "dev").lower()
        config_path = base_dir / "config" / f"config_{env}.yaml"

        if not os.path.exists(config_path):
            logger.error(f"No existe el archivo de configuración: {config_path}")
            raise FileNotFoundError(f"No existe el archivo de configuración: {config_path}")

        # Cargar YAML con envyaml (hace el reemplazo automático)
        logger.debug(f"Cargando configuración desde: {config_path}")
        cfg = EnvYAML(config_path, strict=False)
        return dict(cfg)

    except FileNotFoundError as e:
        logger.error(f"No se encontró el archivo: {e}")
        raise
    except Exception as e:
        logger.error(f"Error al cargar configuración: {e}")
        raise


def asegurar_directorio_sftp(sftp, ruta_completa):

    partes = ruta_completa.strip('/').split('/')
    path_actual = ''
    for parte in partes:
        path_actual += '/' + parte
        try:
            sftp.stat(path_actual)
        except FileNotFoundError:
            logger.debug(f"Creando carpeta: {path_actual}")
            sftp.mkdir(path_actual)


def traerjson(archivo='',valor=None):
    
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / archivo

    with open(config_path, 'r',encoding='utf-8') as file:
        datos = json.load(file)
        # Imprimir los datos cargados
        if (valor):
            return datos[valor]
        else:
            return datos



def borrar_ruta(ruta: str):
    """
    Borra el archivo o carpeta indicada.
    Si se pasa la ruta de un archivo, borra ese archivo.
    Si se pasa la ruta de una carpeta, borra la carpeta completa y su contenido.

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
        


 # Funciones especificos de SFTP energia:
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
    #si nos pasan un nombre, generamos el perido anterior al actual, y si nos pasan el periodo, sería con este periodo
    if not periodo:
        hoy = date.today()
        ultimo_dia_mes_anterior = hoy.replace(day=1) - timedelta(days=1)
        periodo = f"{ultimo_dia_mes_anterior.year}{ultimo_dia_mes_anterior.month:02d}"
    nombre_archivo=f"{basearchivo}_{periodo}{tipo or '.xlsx'}"
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
    """_summary_

    Args:
        lista_archivos (List[str]): _description_
        basearchivo (Optional[str], optional): _description_. Defaults to None.
        periodo (Optional[str], optional): _description_. Defaults to None.
        tipo (Optional[str], optional): _description_. Defaults to None.

    Returns:
        foramto_periodo(e).xlsx
        ejemplo_1225(e).xslx
    """
    #si nos pasan un periodo generamos el perido anterior al actual formato messaño(año en dos digitos) ejem: 0225, y si nos pasan el periodo, sería con este periodo
    if not periodo:
        hoy = date.today()
        ultimo_dia_mes_anterior = hoy.replace(day=1) - timedelta(days=1)
        # Formato messaño con año en dos dígitos, p. ej. 0225
        periodo = f"{ultimo_dia_mes_anterior.month:02d}{ultimo_dia_mes_anterior.year % 100:02d}"
    # si se pasó periodo, se usa tal cual
    nombre_archivo = f"{basearchivo}-{periodo}(e){tipo or '.xlsx'}"
    
    if nombre_archivo not in lista_archivos:
        logger.error(f"No hay archivo a extraer: {nombre_archivo}")
        raise FileNotFoundError(f"No hay archivo a extraer: {nombre_archivo}")
    return nombre_archivo



#Crea carpeta si no existe
def crearcarpeta(local_dir: str):
    try:
        os.makedirs(local_dir, exist_ok=True)
        logger.info(f"Carpeta creada exitosamente: {local_dir}")
    except FileExistsError:
        logger.info("La carpeta destino ya existe, no se crea")
    except Exception as e:
        logger.error(f"No se puede crear la carpeta {local_dir}: {e}")
        raise


def load_settings(overrides: Optional[Dict[str, Any]] = None):
    """
    Genera un TeleowsSettings aplicando overrides opcionales.
    Compatible con módulos migrados desde teleows.
    """
    from energiafacilities.teleows_config import TeleowsSettings
    overrides = overrides or {}
    settings = TeleowsSettings.load_with_overrides(overrides)
    logger.debug("TeleowsSettings generado con overrides: %s", sorted(overrides.keys()))
    return settings


