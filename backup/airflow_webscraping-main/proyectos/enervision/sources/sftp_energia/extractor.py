
from core.base_stractor import BaseExtractorSFTP
from core import utils

def extraersftp_energia():
    config = utils.load_config()
    sftp_config = config.get("sftp_energia", {})
    Extractor = BaseExtractorSFTP(
        config=sftp_config
    )

    conectividad=Extractor.validar_conexion()
    if not (conectividad['status']=="success"):
       return conectividad
    camposvalidos=Extractor.validate()
    if not (camposvalidos['status']=="success"):
        return camposvalidos
    metastraccion=Extractor.extract()
    return metastraccion
