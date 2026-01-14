import logging

from core.base_loader import BaseLoaderPostgres
from core.utils import load_config

logger = logging.getLogger(__name__)


def load_clienteslibres(filepath=None):
    """Carga datos de clientes libres a PostgreSQL."""
    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get("clientes_libres", {})
    Loader = BaseLoaderPostgres(
        config=postgres_config,
        configload=general_config
    )

    Loader.validar_conexion()

    # Validar config necesaria
    local_dest = general_config.get('local_destination_dir')
    table_name = general_config.get('table')

    if not local_dest:
        logger.error("Configuración 'local_destination_dir' no encontrada en clientes_libres")
        raise ValueError("Configuración 'local_destination_dir' no encontrada en clientes_libres")

    Loader.verificar_datos(data=local_dest, table_name=table_name)

    filedata = filepath or local_dest
    carga = Loader.load_data(data=filedata, table_name=table_name)
    return carga

