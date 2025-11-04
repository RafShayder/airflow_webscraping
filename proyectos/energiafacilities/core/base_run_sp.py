import logging

from core.base_postgress import PostgresConnector
from core.utils import load_config

logger = logging.getLogger(__name__)

def run_sp(configyaml: str,configpostgress:str="postgress",sp_name:str='sp_carga'): #sftp_base_sitios
    """ 
        Ejecuta un SP y funct de errores en la base de datos Postgres.
        configyaml: Nombre de la sección en el archivo de configuración YAML que contiene los parámetros generales.
        configpostgress: Nombre de la sección en el archivo de configuración YAML que contiene los parámetros de conexión a Postgres.

    """
    try:
        config = load_config()
        postgres_config = config.get(configpostgress, {})
        general_config = config.get(configyaml, {})
    
        # Crear instancia de conexión
        postgress = PostgresConnector(postgres_config)
        
        sp_ejecutar=general_config[sp_name]
        logger.info(f"Ejecutando SP {sp_ejecutar}")
        postgress.ejecutar(sp_ejecutar, tipo='sp')
        data=postgress.ejecutar("public.log_sp_ultimo_fn",parametros=(f'{sp_ejecutar}()',),tipo='fn')
        logger.info(f"Estado SP: {data['estado'].values}, Detalle: {data['msj_error'].values}")
    except Exception as e:
        logger.error(f"Se produjo un error al ejecutar el sp: {e}")
        raise