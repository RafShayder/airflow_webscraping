from core.base_loader import BaseLoaderPostgres
from core.utils import traerjson,load_config

def load_sftp_energia(filepath=None, table_name=None):
    
    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get("sftp_energia", {})
    Loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=general_config
        )

    Loader.validar_conexion()
    columnas =traerjson(archivo='config/columnas/columns_map.json',valor='tablarecibosenergia')
    filedata= filepath or (general_config['local_dir'] +'/'+ general_config['specific_filename'])
    print("paso ", filedata)
    Loader.verificar_datos(data=filedata ,column_mapping=columnas, table_name=general_config[table_name])
    
    carga=Loader.load_data(data=filedata, column_mapping=columnas, table_name=general_config[table_name]) 
    return carga


def load_sftp_energia_DA(filepath=None):
    return load_sftp_energia(filepath=filepath, table_name='table_DA')

def load_sftp_energia_PD(filepath=None):
    return load_sftp_energia(filepath=filepath, table_name='table_PD')