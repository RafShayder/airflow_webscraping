import logging

from core.base_stractor import BaseExtractorSFTP
from core.utils import load_config
from core.helpers import archivoespecifico_periodo

logger = logging.getLogger(__name__)


def extraer_basedesitios():
    config = load_config()
    sftp_config_connect = config.get("sftp_daas_c", {})
    sftp_config_others =  config.get("sftp_base_sitios", {})
    Extractor = BaseExtractorSFTP(
        config_connect=sftp_config_connect,
        config_paths=sftp_config_others
    )
    Extractor.validar_conexion()
    Extractor.validate()  # validar datos del sftp
    archivos = Extractor.listar_archivos()

    # Validar que exista la config del archivo específico
    specific_filename = sftp_config_others.get("specific_filename")
    if not specific_filename:
        logger.error("Configuración 'specific_filename' no encontrada en sftp_base_sitios")
        raise ValueError("Configuración 'specific_filename' no encontrada en sftp_base_sitios")

    nombrearchivoextraer = archivoespecifico_periodo(lista_archivos=archivos, basearchivo=specific_filename)
    metastraccion = Extractor.extract(specific_file=nombrearchivoextraer)

    if not isinstance(metastraccion, dict) or "ruta" not in metastraccion:
        logger.error("Extracción base sitios no retornó resultado válido: %s", metastraccion)
        raise RuntimeError("Extracción SFTP base sitios no retornó ruta de archivo")
    return metastraccion["ruta"]
