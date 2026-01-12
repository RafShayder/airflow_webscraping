from .base_postgress import PostgresConnector
from .utils import load_config
from .base_exporters import FileExporter
import logging
logger = logging.getLogger(__name__)

def get_save_errors(configyaml: str, table_name='table',configpostgress:str="postgress",filename="data_errors.xlsx"): #sftp_energia
    logger.debug("Generando configuracion para guardar errores de webindra energia")
    logger.debug("Buscando datos con errores")
    config = load_config()
    postgres_config = config.get(configpostgress, {})
    general_config = config.get(configyaml, {})
    
    # Validar que errorconfig existe
    if 'errorconfig' not in general_config:
        logger.warning(f"No se encontró 'errorconfig' en la configuración de '{configyaml}'. Usando valores por defecto.")
        general_config['errorconfig'] = {
            "schema": "PUBLIC",
            "table": "error_energia_ultimo_lote",
            "remote_dir": "/daas/dev/energy-facilities/errors"
        }
    
    fnerror_energia = (
        f"{general_config['errorconfig']['schema']}.{general_config['errorconfig']['table']}"
    )
    tablaorigen = f"{general_config['schema']}.{general_config[table_name]}"

    # Crear instancia de conexión
    with PostgresConnector(postgres_config) as postgress:
        data = postgress.ejecutar(fnerror_energia, tipo='fn', parametros=(tablaorigen,))

    # si es vacio no imprime nada
    if not data.empty:
        baseexporter = FileExporter()
        condigsfpt = config.get("sftp_daas_c", {})
        baseexporter.export_dataframe_to_remote(
            data,
            conn=condigsfpt,
            remote_dir=general_config['errorconfig']["remote_dir"],
            filename=filename,
        )
        logger.debug(
            "Se generó archivo de errores en %s",
            general_config["errorconfig"]["remote_dir"],
        )
    else:
        logger.debug("no hay errores %s.", tablaorigen)

    return data
