from core.base_loader import BaseLoaderPostgres
from core.utils import traerjson,load_config

def load_sftp_base_sitos(filepath=None,): #sftp_base_sitios, tablabasedesitios, Base de Sitios
    
    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get("sftp_pago_energia", {})
    Loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=general_config
        )

    Loader.validar_conexion()

    filedata= filepath or (general_config['local_dir'] +'/'+ general_config['nombre_salida_local'])

    carga=Loader.load_data(data=filedata,)
    return carga

