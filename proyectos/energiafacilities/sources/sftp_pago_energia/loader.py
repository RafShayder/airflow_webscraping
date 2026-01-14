import logging

from core.base_loader import BaseLoaderPostgres
from core.utils import traerjson, load_config

logger = logging.getLogger(__name__)

def load_sftp_pago_energia(filepath=None):
    """Carga datos de pago energía desde archivo consolidado a PostgreSQL."""
    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get("sftp_pago_energia", {})
    Loader = BaseLoaderPostgres(
        config=postgres_config,
        configload=general_config
    )

    Loader.validar_conexion()

    # Construir filepath desde config si no se proporciona
    if not filepath:
        local_dir = general_config.get('local_dir')
        nombre_salida = general_config.get('nombre_salida_local')
        if not local_dir or not nombre_salida:
            logger.error("Configuración incompleta: falta local_dir o nombre_salida_local en sftp_pago_energia")
            raise ValueError("Configuración incompleta: falta local_dir o nombre_salida_local en sftp_pago_energia")
        filedata = f"{local_dir}/{nombre_salida}"
    else:
        filedata = filepath

    carga = Loader.load_data(data=filedata)
    return carga

