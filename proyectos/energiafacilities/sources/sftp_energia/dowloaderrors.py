from requests import post
from core.base_exporters import FileExporter
from core.utils import load_config,setup_logging
from core.base_postgress import PostgresConnector
setup_logging("INFO")
def generarlog_sftp():
    config = load_config()
    postgres_config = config.get("postgress", {})
    sftpdass = config.get("sftp_daas_c", {})
    error_tabla = config.get("sftp_energia",{})
    postgres= PostgresConnector(postgres_config)
    dferrores= postgres.extract(error_tabla)
    exportarfile= FileExporter()
    exportarfile.export_dataframe_to_remote(dferrores,sftpdass,"sftp_energia")
    
    

generarlog_sftp()