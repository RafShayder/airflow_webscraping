import logging

from core.base_stractor import BaseExtractorSFTP
from core.utils import load_config
from core.helpers import archivoespecifico_periodo_CL

logger = logging.getLogger(__name__)


def extraersftp_clienteslibres():
    config = load_config()
    sftp_config_connect = config.get("sftp_daas_c", {})
    sftp_config_others =  config.get("clientes_libres", {})
    Extractor = BaseExtractorSFTP(
        config_connect=sftp_config_connect,
        config_paths=sftp_config_others
    )
    Extractor.validar_conexion()
    Extractor.validate()  # validar datos del sftp
    archivos_atributos = Extractor.listar_archivos()

    # Validar que exista la config del archivo específico
    specific_filename = sftp_config_others.get("specific_filename")
    if not specific_filename:
        logger.error("Configuración 'specific_filename' no encontrada en clientes_libres")
        raise ValueError("Configuración 'specific_filename' no encontrada en clientes_libres")

    nombrearchivoextraer = archivoespecifico_periodo_CL(lista_archivos=archivos_atributos, basearchivo=specific_filename)
    metastraccion = Extractor.extract(specific_file=nombrearchivoextraer)

    if not isinstance(metastraccion, dict) or "ruta" not in metastraccion:
        logger.error("Extracción clientes libres no retornó resultado válido: %s", metastraccion)
        raise RuntimeError("Extracción SFTP clientes libres no retornó ruta de archivo")
    return metastraccion["ruta"] 

