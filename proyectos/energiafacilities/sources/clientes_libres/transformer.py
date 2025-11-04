from .help.transform_helpers import ejecutar_transformacion
from core.utils import traerjson,load_config

def transformer_clienteslibres(filepath=None):
    
    config = load_config()
    general_config = config.get("clientes_libres", {})
    mapeo_campos =traerjson(archivo='config/columnas/transformacion.json',valor='clienteslibres')
    
    resultado = ejecutar_transformacion(general_config, mapeo_campos, filepath=filepath, save=True)
    return resultado



