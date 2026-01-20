import logging

from core.base_loader import BaseLoaderPostgres
from core.utils import load_config
from core.helpers import traerjson

logger = logging.getLogger(__name__)


def load_sftp_base_sitios(config_name, jsontablanames, sheetname, filepath=None):
    """Carga datos de base sitios desde Excel a PostgreSQL."""
    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get(config_name, {})
    Loader = BaseLoaderPostgres(
        config=postgres_config,
        configload=general_config
    )

    Loader.validar_conexion()
    columnas = traerjson(archivo='config/columnas/columns_map.json', valor=jsontablanames)

    # Construir filepath desde config si no se proporciona
    if not filepath:
        local_dir = general_config.get('local_dir')
        specific_filename = general_config.get('specific_filename')
        if not local_dir or not specific_filename:
            logger.error("Configuración incompleta: falta local_dir o specific_filename en %s", config_name)
            raise ValueError(f"Configuración incompleta: falta local_dir o specific_filename en {config_name}")
        filedata = f"{local_dir}/{specific_filename}"
    else:
        filedata = filepath

    Loader.verificar_datos(data=filedata, column_mapping=columnas, sheet_name=sheetname)
    carga = Loader.load_data(data=filedata, column_mapping=columnas, sheet_name=sheetname)

    return carga



def loader_basesitios(filepath: str):
    config_name = 'sftp_base_sitios'
    jsontablanames = 'tablabasedesitios'
    sheetname = 'Base de Sitios'
    carga = load_sftp_base_sitios(config_name, jsontablanames, sheetname, filepath=filepath)
    return carga


def loader_bitacora_basesitios(filepath: str):
    config_name = 'sftp_base_sitios_bitacora'
    jsontablanames = 'tablabasedesitiosbitacora'
    sheetname = 'Bitacora'
    carga = load_sftp_base_sitios(config_name, jsontablanames, sheetname, filepath=filepath)
    return carga




