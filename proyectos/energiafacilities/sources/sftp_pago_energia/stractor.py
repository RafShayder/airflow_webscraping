import logging

from core.base_stractor import BaseExtractorSFTP
from core.utils import load_config, traerjson

logger = logging.getLogger(__name__)

def extraersftp_pago_energia():
    config = load_config()
    sftp_config_connect = config.get("sftp_energia_c", {})
    sftp_config_others = config.get("sftp_pago_energia", {})
    Extractor = BaseExtractorSFTP(
        config_connect=sftp_config_connect,
        config_paths=sftp_config_others
    )
    Extractor.validar_conexion()
    Extractor.validate() #validar datos del sftp
    archivos = Extractor.listar_archivos()
    columnas = traerjson(archivo='config/columnas/columns_map_pago_energia.json', valor="tablarpagoenergia")
    metastraccion = Extractor.estract_archivos_excel(archivos=archivos, nombre_salida_local="Consolidado_PagoEnergia.xlsx", fila_inicio=6, columnas_verificar=columnas)
    if not isinstance(metastraccion, dict) or "ruta" not in metastraccion:
        logger.error("Extracción pago energía no retornó resultado válido: %s", metastraccion)
        raise RuntimeError("Extracción SFTP pago energía no retornó ruta de archivo")
    return metastraccion["ruta"]




