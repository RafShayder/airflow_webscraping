from core.base_loader import BaseLoaderPostgres
from core.utils import load_config
from core.helpers import traerjson

def load_sftp_base_sitos(config_name,jsontablanames,sheetname ,filepath=None,): #sftp_base_sitios, tablabasedesitios, Base de Sitios
    
    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get(config_name, {})
    Loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=general_config
        )

    Loader.validar_conexion()
    columnas =traerjson(archivo='config/columnas/columns_map.json',valor=jsontablanames)
    filedata= filepath or (general_config['local_dir'] +'/'+ general_config['specific_filename'])
    Loader.verificar_datos(data=filedata ,column_mapping=columnas,sheet_name=sheetname)
    
    carga=Loader.load_data(data=filedata, column_mapping=columnas,sheet_name=sheetname)
    
    return carga



def loader_basesitios(filepath: str):
    config_name='sftp_base_sitios'
    jsontablanames='tablabasedesitios'
    sheetname='Base de Sitios'
    carga=load_sftp_base_sitos(config_name,jsontablanames,sheetname,filepath=filepath)
    return carga
def loader_bitacora_basesitios(filepath: str):
    config_name='sftp_base_sitios_bitacora'
    jsontablanames='tablabasedesitiosbitacora'
    sheetname='Bitacora'
    carga=load_sftp_base_sitos(config_name,jsontablanames,sheetname,filepath=filepath)
    return carga




