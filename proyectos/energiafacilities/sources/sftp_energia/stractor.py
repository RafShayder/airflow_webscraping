import logging

from core.base_stractor import BaseExtractorSFTP
from core.utils import load_config, setup_logging
from core.helpers import generar_archivo_especifico
setup_logging(level="INFO")
logger = logging.getLogger(__name__)

def extraersftp_energia(specific_file_config: str ,periodo: str=None):
    config = load_config()
    sftp_config_connect = config.get("sftp_energia_c", {})
    sftp_config_others =  config.get("sftp_energia", {})
    Extractor = BaseExtractorSFTP(
        config_connect=sftp_config_connect,
        config_paths=sftp_config_others
    )
    Extractor.validar_conexion()
    Extractor.validate() #validar datos del sftp
    archivos_atributos= Extractor.listar_archivos_atributos()
    archivoextraer = generar_archivo_especifico(
        lista_archivos=archivos_atributos,
        basearchivo=sftp_config_others[specific_file_config],
        periodo=periodo,
    )
    if not archivoextraer:
        msg = "No se encontraron archivos para extraer en SFTP energía"
        logger.error(msg)
        raise FileNotFoundError(msg)
    metastraccion = Extractor.extract(specific_file=archivoextraer["nombre"])
    return metastraccion

def extraersftp_energia_PD(periodo: str=None):
    metastraccion = extraersftp_energia("specific_filename", periodo=periodo)
    return metastraccion.get("ruta") if isinstance(metastraccion, dict) else None

def extraersftp_energia_DA(periodo: str=None):
    metastraccion = extraersftp_energia("specific_filename2", periodo=periodo)
    return metastraccion.get("ruta") if isinstance(metastraccion, dict) else None
