from __future__ import annotations
import logging
from envyaml import EnvYAML
from pathlib import Path
from dotenv import load_dotenv
import os
import io
import shutil
import json
from datetime import date, timedelta, datetime
import re
from typing import List, Optional, Any, Dict
from types import SimpleNamespace
import pandas as pd
import paramiko
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
        # Cargar variables del .env si existe (opcional)
        base_dir = Path(__file__).resolve().parent.parent
        env_file = base_dir / ".env"
        
        # Cargar .env desde el directorio base del proyecto
        if env_file.exists():
            load_dotenv(dotenv_path=env_file)
        else:
            # Si no existe en el base_dir, intentar desde el directorio actual (fallback)
            load_dotenv()
        
        env = env or os.getenv("ENV_MODE", "dev").lower()
        config_path = base_dir / "config" / f"config_{env}.yaml"
        #config_path = f"config/config_{env}.yaml"
        
        if not os.path.exists(config_path):
            logger.error(f"No existe el archivo de configuración: {config_path}")
            raise 
        # Cargar YAML con envyaml (hace el reemplazo automático)
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
            a=sftp.stat(path_actual) 
        except FileNotFoundError:
            logger.info(f"Creando carpeta: {path_actual}")
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
            logger.info(f"Archivo eliminado: {ruta}")

        elif os.path.isdir(ruta):
            shutil.rmtree(ruta)
            logger.info(f"Carpeta eliminada con todo su contenido: {ruta}")
     

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


#Crea carpeta si no existe 
def crearcarpeta(local_dir: str):
    try:
        os.makedirs(local_dir, exist_ok=True)
    except FileExistsError:
        logger.info("La carpeta destino ya existe, no se crea")
        pass
    finally:
        logger.error("No se puede crear la carpeta")
        raise

