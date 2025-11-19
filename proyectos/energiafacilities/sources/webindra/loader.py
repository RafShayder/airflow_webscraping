from energiafacilities.core.base_loader import BaseLoaderPostgres
from energiafacilities.core.utils import load_config
from energiafacilities.core.helpers import traerjson

def load_indra(filepath=None):
    
    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get("webindra_energia", {})
    Loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=general_config
        )

    Loader.validar_conexion()
    columnas =traerjson(archivo='config/columnas/columns_map.json',valor='tablareciboswebindra')
    
    Loader.verificar_datos(data=general_config['local_dir'] +'/'+ general_config['specific_filename'] ,column_mapping=columnas, strictreview=False,numerofilasalto=2)
    
    if not (filepath):
        filepath=general_config['local_dir'] +'/'+ general_config['specific_filename']
    carga=Loader.load_data(data=filepath, column_mapping=columnas ,numerofilasalto=2)
    return carga




