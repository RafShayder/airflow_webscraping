import logging

from core.base_loader import BaseLoaderPostgres
from core.utils import load_config
from core.helpers import traerjson

logger = logging.getLogger(__name__)

def load_sftp_energia(filepath=None, table_name=None):
    
    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get("sftp_energia", {})
    Loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=general_config
        )

    Loader.validar_conexion()
    columnas = traerjson(archivo='config/columnas/columns_map.json', valor='tablarecibosenergia')

    # Construir filepath desde config si no se proporciona
    if not filepath:
        local_dir = general_config.get('local_dir')
        specific_filename = general_config.get('specific_filename')
        if not local_dir or not specific_filename:
            logger.error("Configuración incompleta: falta local_dir o specific_filename en sftp_energia")
            raise ValueError("Configuración incompleta: falta local_dir o specific_filename en sftp_energia")
        filedata = f"{local_dir}/{specific_filename}"
    else:
        filedata = filepath

    # Validar que table_name exista en config
    if not table_name:
        logger.error("table_name es requerido para load_sftp_energia")
        raise ValueError("table_name es requerido para load_sftp_energia")

    target_table = general_config.get(table_name)
    if not target_table:
        logger.error("Configuración '%s' no encontrada en sftp_energia", table_name)
        raise ValueError(f"Configuración '{table_name}' no encontrada en sftp_energia")

    Loader.verificar_datos(data=filedata, column_mapping=columnas, table_name=target_table)
    carga = Loader.load_data(data=filedata, column_mapping=columnas, table_name=target_table)
    return carga


def load_sftp_energia_DA(filepath=None):
    return load_sftp_energia(filepath=filepath, table_name='table_DA')

def load_sftp_energia_PD(filepath=None):
    return load_sftp_energia(filepath=filepath, table_name='table_PD')