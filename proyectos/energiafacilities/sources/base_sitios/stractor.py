from core.base_stractor import BaseExtractorSFTP
from core.utils import  load_config,archivoespecifico_periodo


def extraer_basedesitios():
    config = load_config()
    sftp_config_connect = config.get("sftp_daas_c", {})
    sftp_config_others =  config.get("sftp_base_sitios", {})
    Extractor = BaseExtractorSFTP(
        config_connect=sftp_config_connect,
        config_paths=sftp_config_others
    )
    Extractor.validar_conexion()
    Extractor.validate() #validar datos del sftp
    archivos_atributos= Extractor.listar_archivos()
    nombrearchivoextraer=archivoespecifico_periodo(lista_archivos=archivos_atributos,basearchivo=sftp_config_others["specific_filename"])
    metastraccion=Extractor.extract(specific_file=nombrearchivoextraer)
    return metastraccion['ruta']
