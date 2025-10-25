import os
from pathlib import Path
from dotenv import load_dotenv
from envyaml import EnvYAML

# Cargar variables de entorno desde el .env del proyecto
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ENV = BASE_DIR / ".env"
if PROJECT_ENV.exists():
    load_dotenv(PROJECT_ENV, override=False)
else:
    load_dotenv()

# Permite definir perfiles en env.yaml (por defecto "default")
ENV_PROFILE = os.getenv("TELEOWS_ENV", "default")
YAML_PATH = BASE_DIR / "env.yaml"
if YAML_PATH.exists():
    try:
        yaml_content = EnvYAML(YAML_PATH, strict=False)
        yaml_section = yaml_content.get(ENV_PROFILE, yaml_content)
        if isinstance(yaml_section, dict):
            for key, value in yaml_section.items():
                if isinstance(value, (dict, list)):
                    continue
                os.environ.setdefault(key.upper(), str(value))
    except Exception as exc:
        raise RuntimeError(f"No se pudo cargar {YAML_PATH}: {exc}") from exc

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
# Para Airflow: usa /opt/airflow/proyectos/teleows/temp por defecto
# Para desarrollo local: usa Downloads/scraper_downloads
default_path = "/opt/airflow/proyectos/teleows/temp" if os.path.exists("/opt/airflow") else str(Path.home() / "Downloads" / "scraper_downloads")
DOWNLOAD_PATH = Path(os.getenv("DOWNLOAD_PATH", default_path)).expanduser()
# Intentar crear el directorio si no existe (ignora errores de permisos)
try:
    DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)
except OSError:
    pass
# Convertir a ruta absoluta para Chrome
DOWNLOAD_PATH = str(DOWNLOAD_PATH.resolve())

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
