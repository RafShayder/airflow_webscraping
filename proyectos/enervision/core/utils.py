from __future__ import annotations
import logging
from envyaml import EnvYAML #type: ignore
from pathlib import Path
from dotenv import load_dotenv
import os
from core.exceptions import ConfigError #agregar las excepciones
import json
def osraiz() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def setup_logging(level: str = None) -> None:
    """
    Configura logging. Si no se pasa level, busca en:
    1. Airflow Variable LOG_LEVEL
    2. Variable de entorno LOG_LEVEL
    3. Por defecto: INFO
    """
    if level is None:
        # Intentar desde Airflow Variable
        try:
            from airflow.models import Variable
            level = Variable.get("LOG_LEVEL", default_var=None)
        except:
            pass

        # Si no existe en Airflow, usar variable de entorno
        if not level:
            level = os.getenv("LOG_LEVEL", "INFO")

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )


class ConfigError(Exception):
    """Excepción personalizada para errores de configuración."""
    pass

def load_config(env: str | None = None) -> dict:
    """
    Carga un archivo YAML con soporte automático para variables de entorno .
    Ejemplo de uso en el YAML:
        postgres:
          user: ${POSTGRES_USER}
          password: ${POSTGRES_PASS}

    Si las variables existen en el entorno, se reemplazan automáticamente.

    """
    try:
        # Cargar variables del .env si existe (opcional)

        env_file = Path(__file__).resolve().parent.parent / ".env"
        if env_file.exists():
            load_dotenv(env_file, override=False)
        else:
            load_dotenv()
        env = env or os.getenv("ENV_MODE", "dev").lower()
        base_dir = Path(__file__).resolve().parent.parent
        config_path = base_dir / "config" / f"config_{env}.yaml"
        
        if not os.path.exists(config_path):
            raise ConfigError(f"No existe el archivo de configuración: {config_path} - base dir: {base_dir}")
        # Cargar YAML con envyaml (hace el reemplazo automático)
        cfg = EnvYAML(config_path, strict=False)
        return dict(cfg)

    except FileNotFoundError as e:
        raise ConfigError(f"No se encontró el archivo: {e}") from e
    except Exception as e:
        raise ConfigError(f"Error al cargar configuración: {e}") from e



def asegurar_directorio_sftp(sftp, ruta_completa):

    partes = ruta_completa.strip('/').split('/')
    path_actual = ''
    for parte in partes:
        path_actual += '/' + parte
        try:
            a=sftp.stat(path_actual) 
        except FileNotFoundError:
            print(f"Creando carpeta: {path_actual}")
            sftp.mkdir(path_actual)


def traerjson(archivo='',valor=None):
    
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / archivo

    with open(config_path, 'r') as file:
        datos = json.load(file)
        # Imprimir los datos cargados
        if (valor):
            return datos[valor]
        else:
            return datos

def cofiguracion_standar():
    # modo dev modo prod 
    return 
