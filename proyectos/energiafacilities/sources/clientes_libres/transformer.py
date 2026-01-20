import logging

from sources.clientes_libres.help.transform_helpers import ejecutar_transformacion
from core.utils import load_config
from core.helpers import traerjson

logger = logging.getLogger(__name__)


def transformer_clienteslibres(filepath=None):
    
    config = load_config()
    general_config = config.get("clientes_libres", {})
    mapeo_campos =traerjson(archivo='config/columnas/transformacion.json',valor='clienteslibres')
    
    df = ejecutar_transformacion(general_config, mapeo_campos,save=True, filepath=filepath)
    return df



