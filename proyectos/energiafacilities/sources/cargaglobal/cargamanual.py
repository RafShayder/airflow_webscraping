from os import replace
from core.base_loader import BaseLoaderPostgres
from core.utils import load_config,setup_logging

setup_logging(level="INFO")

def load_clienteslibres(filepath="tmp/global/archivo.xlsx", modo=None,schema=None, table_name=None):

    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = {}
    Loader = BaseLoaderPostgres(
        config=postgres_config,
        configload=general_config
    )

    carga=Loader.load_data(data=filepath,modo=modo,schema=schema, table_name=table_name)
    return carga

'''
filepath="tmp/sftp_clientes_libres/processed/clientes_libres.xlsx"
load_clienteslibres(filepath=filepath, modo='replace',schema='raw', table_name='sftp_mm_clientes_libres')
'''


