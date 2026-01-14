# Script temporal para pruebas locales
from sources.sftp_base_suministros_activos.stractor import extraer_base_suministros_activos
from sources.sftp_base_suministros_activos.loader import load_base_suministros_activos
from core.utils import setup_logging

setup_logging()

# Extraer archivo desde SFTP
pathextraida = extraer_base_suministros_activos()
print(f"Archivo extraído: {pathextraida}")

# Cargar datos a PostgreSQL
load_base_suministros_activos(filepath=pathextraida)

