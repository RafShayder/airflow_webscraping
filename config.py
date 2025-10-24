import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde archivo .env
load_dotenv()

# Configuración de credenciales
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Validar que las credenciales estén configuradas
if not USERNAME or not PASSWORD:
    raise ValueError(
        "Credenciales no configuradas. "
        "Por favor configura USERNAME y PASSWORD en el archivo .env"
    )

# Configuración de descarga
DOWNLOAD_PATH = os.getenv("DOWNLOAD_PATH", str(Path.home() / "Downloads" / "scraper_downloads"))
# Convertir a ruta absoluta para Chrome
DOWNLOAD_PATH = str(Path(DOWNLOAD_PATH).resolve())

# Configuración de la aplicación
MAX_IFRAME_ATTEMPTS = int(os.getenv("MAX_IFRAME_ATTEMPTS", "60"))
MAX_STATUS_ATTEMPTS = int(os.getenv("MAX_STATUS_ATTEMPTS", "60"))

# Opciones a seleccionar (desde .env o por defecto)
options_str = os.getenv("OPTIONS_TO_SELECT", "CM,OPM")
OPTIONS_TO_SELECT = [opt.strip() for opt in options_str.split(",")]

# Configuración de fechas
DATE_MODE = int(os.getenv("DATE_MODE", "2"))  # 1: Fechas manuales | 2: Último mes
DATE_FROM = os.getenv("DATE_FROM", "2025-09-01")
DATE_TO = os.getenv("DATE_TO", "2025-09-10")

# Nombres personalizados para los archivos exportados
GDE_OUTPUT_FILENAME = os.getenv("GDE_OUTPUT_FILENAME")
DYNAMIC_CHECKLIST_OUTPUT_FILENAME = os.getenv("DYNAMIC_CHECKLIST_OUTPUT_FILENAME")

# Política de manejo cuando el archivo destino ya existe (true = reemplazar)
EXPORT_OVERWRITE_FILES = os.getenv("EXPORT_OVERWRITE_FILES", "true").strip().lower() == "true"
