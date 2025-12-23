from core.base_loader import BaseLoaderPostgres
from core.utils import load_config
from core.helpers import traerjson

def load_neteco(filepath=None):
    
    """
    Carga datos de Neteco desde la ruta transformada a una base de datos PostgreSQL.
    
    Args:
        filepath (str): Ruta al archivo. Si no se proporciona, se carga desde la ruta especificada en la configuración.
        table_name (str): Nombre de la tabla en la que se cargarán los datos (opcional). Si no se proporciona, se carga en la tabla especificada en la configuración.
    
    Returns:
        json: Resultado de la operación de carga.
    """
    
    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get("neteco", {})
    Loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=general_config
        )

    Loader.validar_conexion()
    
    columnas =traerjson(archivo='config/columnas/columns_map_neteco.json',valor='neteco')
    filedata= filepath or general_config['local_destination_dir']
  
    Loader.verificar_datos(data=filedata ,column_mapping=columnas)

    carga=Loader.load_data(data=filedata, column_mapping=columnas) 
    return carga