import logging

from .base_postgress import PostgresConnector
from .utils import load_config

logger = logging.getLogger(__name__)

def run_sp(configyaml: str,configpostgress:str="postgress",sp_name:str='sp_carga', sp_value:str=None): #sftp_base_sitios
    """
        Ejecuta un SP y funct de errores en la base de datos Postgres.
        configyaml: Nombre de la sección en el archivo de configuración YAML que contiene los parámetros generales.
        configpostgress: Nombre de la sección en el archivo de configuración YAML que contiene los parámetros de conexión a Postgres.

        sp_value: cuando no quieres usar la config, sino directgamente el nombre del sp, no es necesario el configyaml, pasar vacio
    """
    config = load_config()
    postgres_config = config.get(configpostgress, {})
    general_config = config.get(configyaml, {})

    # Usar context manager para garantizar cierre de conexión
    with PostgresConnector(postgres_config) as postgress:
        sp_ejecutar = sp_value or general_config.get(sp_name)
        if not sp_ejecutar:
            logger.error("No se encontró el SP '%s' en la configuración '%s'", sp_name, configyaml)
            raise ValueError(f"No se encontró el SP '{sp_name}' en la configuración '{configyaml}'")
        logger.info(f"Ejecutando SP: {sp_ejecutar}")
        postgress.ejecutar(sp_ejecutar, tipo='sp')
        data = postgress.ejecutar("public.log_sp_ultimo_fn", parametros=(f'{sp_ejecutar}()',), tipo='fn')
        logger.debug(f"Estado SP: {data['estado'].values}, Detalle: {data['msj_error'].values}")
        logger.info(f"SP ejecutado correctamente: {sp_ejecutar}")