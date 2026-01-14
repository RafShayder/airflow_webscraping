import logging
from core.base_loader import BaseLoaderPostgres
from core.utils import load_config
from core.helpers import traerjson

logger = logging.getLogger(__name__)

def load_indra(filepath=None):

    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get("webindra_energia", {})
    Loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=general_config
        )

    Loader.validar_conexion()
    columnas = traerjson(archivo='config/columnas/columns_map.json', valor='tablareciboswebindra')

    # Construir filepath desde config si no se proporciona
    if not filepath:
        local_dir = general_config.get('local_dir')
        specific_filename = general_config.get('specific_filename')
        if not local_dir or not specific_filename:
            logger.error("Configuración incompleta: falta local_dir o specific_filename en webindra_energia")
            raise ValueError("Configuración incompleta: falta local_dir o specific_filename en webindra_energia")
        filepath = f"{local_dir}/{specific_filename}"

    Loader.verificar_datos(data=filepath, column_mapping=columnas, strictreview=False, numerofilasalto=2)

    carga = Loader.load_data(data=filepath, column_mapping=columnas, numerofilasalto=2)
    return carga




