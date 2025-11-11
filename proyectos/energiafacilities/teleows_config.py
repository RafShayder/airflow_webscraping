import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

BASE_DIR = Path(__file__).resolve().parent
# Usar config/config_*.yaml en lugar de settings.yaml para consolidar configuración
CONFIG_DIR = BASE_DIR / "config"

_ENV_FIELD_MAP: Mapping[str, str] = {
    "username": "USERNAME",
    "password": "PASSWORD",
    "download_path": "DOWNLOAD_PATH",
    "max_iframe_attempts": "MAX_IFRAME_ATTEMPTS",
    "max_status_attempts": "MAX_STATUS_ATTEMPTS",
    "options_to_select": "OPTIONS_TO_SELECT",
    "date_mode": "DATE_MODE",
    "date_from": "DATE_FROM",
    "date_to": "DATE_TO",
    "gde_output_filename": "GDE_OUTPUT_FILENAME",
    "dynamic_checklist_output_filename": "DYNAMIC_CHECKLIST_OUTPUT_FILENAME",
    "export_overwrite_files": "EXPORT_OVERWRITE_FILES",
    "proxy": "PROXY",
    "headless": "HEADLESS",
}

_TRUE_VALUES = {"1", "true", "yes", "on"}


def _default_download_path() -> str:
    if Path("/opt/airflow").exists():
        return "/opt/airflow/proyectos/energiafacilities/temp"
    return str(Path.home() / "Downloads" / "scraper_downloads")


def _load_settings_file() -> Dict[str, Any]:
    """
    Carga configuración desde config/config_*.yaml (mismo sistema que otros módulos).
    Extrae las secciones 'teleows', 'gde' y 'dynamic_checklist' y las combina.
    """
    try:
        from envyaml import EnvYAML
    except ImportError:
        # Si EnvYAML no está disponible, intentar con PyYAML básico
        try:
            import yaml
        except ImportError:
            return {}
        
        # Fallback a PyYAML básico
        env = os.getenv("ENV_MODE", "dev").lower()
        config_path = CONFIG_DIR / f"config_{env}.yaml"
        
        if not config_path.exists():
            return {}
        
        with config_path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        
        if not isinstance(raw, dict):
            return {}
        
        # Combinar secciones teleows, gde y dynamic_checklist
        result = {}
        if "teleows" in raw:
            result.update(raw["teleows"])
        
        # Mapear campos específicos de gde y dynamic_checklist
        if "gde" in raw:
            gde_config = raw["gde"]
            result.update(gde_config)
            # Mapear specific_filename a gde_output_filename si existe
            if "specific_filename" in gde_config and "gde_output_filename" not in result:
                result["gde_output_filename"] = gde_config["specific_filename"]
        
        if "dynamic_checklist" in raw:
            dc_config = raw["dynamic_checklist"]
            result.update(dc_config)
            # Mapear specific_filename a dynamic_checklist_output_filename si existe
            if "specific_filename" in dc_config and "dynamic_checklist_output_filename" not in result:
                result["dynamic_checklist_output_filename"] = dc_config["specific_filename"]
        
        return result
    
    # Usar EnvYAML (mismo sistema que core/utils.py)
    # Cargar variables del .env de energiafacilities si existe (antes de EnvYAML)
    try:
        from dotenv import load_dotenv
        dotenv_path = BASE_DIR / ".env"
        if dotenv_path.exists():
            load_dotenv(dotenv_path=dotenv_path, override=False)
        else:
            load_dotenv()  # Buscar en directorio actual o padres
    except ImportError:
        pass  # python-dotenv no está instalado, continuar sin cargar .env
    except Exception:
        pass  # Si hay error al cargar .env, continuar sin él
    
    env = os.getenv("ENV_MODE", "dev").lower()
    config_path = CONFIG_DIR / f"config_{env}.yaml"
    
    if not config_path.exists():
        return {}
    
    try:
        cfg = EnvYAML(config_path, strict=False)
        config_dict = dict(cfg)
        
        # Combinar secciones teleows, gde y dynamic_checklist
        result = {}
        if "teleows" in config_dict:
            result.update(config_dict["teleows"])
        
        # Mapear campos específicos de gde y dynamic_checklist
        if "gde" in config_dict:
            gde_config = config_dict["gde"]
            result.update(gde_config)
            # Mapear specific_filename a gde_output_filename si existe
            if "specific_filename" in gde_config and "gde_output_filename" not in result:
                result["gde_output_filename"] = gde_config["specific_filename"]
        
        if "dynamic_checklist" in config_dict:
            dc_config = config_dict["dynamic_checklist"]
            result.update(dc_config)
            # Mapear specific_filename a dynamic_checklist_output_filename si existe
            if "specific_filename" in dc_config and "dynamic_checklist_output_filename" not in result:
                result["dynamic_checklist_output_filename"] = dc_config["specific_filename"]
        
        return result
    except Exception:
        return {}


def _load_env_overrides() -> Dict[str, Any]:
    # Intentar cargar el .env desde energiafacilities
    try:
        from dotenv import load_dotenv
        dotenv_path = BASE_DIR / ".env"
        if dotenv_path.exists():
            load_dotenv(dotenv_path=dotenv_path, override=False)
    except ImportError:
        pass  # python-dotenv no está instalado, continuar sin cargar .env
    except Exception:
        pass  # Si hay error al cargar .env, continuar sin él

    overrides: Dict[str, Any] = {}
    for field, env_var in _ENV_FIELD_MAP.items():
        # Primero intentar con prefijo TELEOWS_
        value = os.getenv(f"TELEOWS_{env_var}")
        # Si no existe, intentar sin prefijo
        if value is None:
            value = os.getenv(env_var)
        if value is not None:
            overrides[field] = value
    return overrides


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in _TRUE_VALUES


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_options(value: Any, fallback: List[str]) -> List[str]:
    if value is None:
        return list(fallback)
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
        return items or list(fallback)
    if isinstance(value, (list, tuple, set)):
        items = [str(item).strip() for item in value if str(item).strip()]
        return items or list(fallback)
    return list(fallback)


@dataclass(frozen=True)
class TeleowsSettings:
    """Objeto de configuración consumido por los workflows (carga desde config/config_*.yaml)."""
    username: str
    password: str
    download_path: str
    max_iframe_attempts: int = 60
    max_status_attempts: int = 60
    options_to_select: List[str] = field(default_factory=lambda: ["CM", "OPM"])
    date_mode: int = 2
    date_from: str = "2025-09-01"
    date_to: str = "2025-09-10"
    gde_output_filename: Optional[str] = None
    dynamic_checklist_output_filename: Optional[str] = None
    export_overwrite_files: bool = True
    proxy: Optional[str] = None
    headless: bool = False

    @classmethod
    def load(cls) -> "TeleowsSettings":
        raw_settings: Dict[str, Any] = {}
        raw_settings.update(_load_settings_file())
        raw_settings.update(_load_env_overrides())
        return _build_settings(raw_settings)

    @classmethod
    def load_with_overrides(cls, overrides: Dict[str, Any]) -> "TeleowsSettings":
        raw_settings: Dict[str, Any] = {}
        raw_settings.update(_load_settings_file())
        raw_settings.update(_load_env_overrides())  # Cargar variables de entorno antes de overrides
        raw_settings.update(overrides)  # Los overrides de Airflow tienen prioridad final
        return _build_settings(raw_settings)

    @classmethod
    def from_mapping(cls, mapping: Dict[str, Any]) -> "TeleowsSettings":
        return _build_settings(dict(mapping))


def _build_settings(raw_settings: Dict[str, Any]) -> TeleowsSettings:
    """Normaliza el diccionario recibido y instancia TeleowsSettings."""
    username = _as_optional_str(raw_settings.get("username"))
    password = _as_optional_str(raw_settings.get("password"))

    if not username or not password:
        raise ValueError(
            "Credenciales no configuradas. Define USERNAME y PASSWORD (ya sea en variables "
            "de entorno, Airflow o config/config_*.yaml)."
        )

    download_path_setting = _as_optional_str(raw_settings.get("download_path")) or _default_download_path()
    download_path = Path(download_path_setting).expanduser()
    try:
        download_path.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    download_path_str = str(download_path.resolve())

    max_iframe_attempts = _as_int(raw_settings.get("max_iframe_attempts"), 60)
    max_status_attempts = _as_int(raw_settings.get("max_status_attempts"), 60)
    options_to_select = _as_options(raw_settings.get("options_to_select"), ["CM", "OPM"])
    date_mode = _as_int(raw_settings.get("date_mode"), 2)
    date_from = _as_optional_str(raw_settings.get("date_from")) or "2025-09-01"
    date_to = _as_optional_str(raw_settings.get("date_to")) or "2025-09-10"
    gde_output_filename = _as_optional_str(raw_settings.get("gde_output_filename"))
    dynamic_output_filename = _as_optional_str(
        raw_settings.get("dynamic_checklist_output_filename")
    )
    export_overwrite_files = _as_bool(raw_settings.get("export_overwrite_files"), True)
    proxy_value = _as_optional_str(raw_settings.get("proxy"))
    headless_value = _as_bool(raw_settings.get("headless"), False)

    return TeleowsSettings(
        username=username,
        password=password,
        download_path=download_path_str,
        max_iframe_attempts=max_iframe_attempts,
        max_status_attempts=max_status_attempts,
        options_to_select=options_to_select,
        date_mode=date_mode,
        date_from=date_from,
        date_to=date_to,
        gde_output_filename=gde_output_filename,
        dynamic_checklist_output_filename=dynamic_output_filename,
        export_overwrite_files=export_overwrite_files,
        proxy=proxy_value,
        headless=headless_value,
    )


def load_default_settings() -> TeleowsSettings:
    """
    Helper opcional para scripts locales.

    Equivalente a ``TeleowsSettings.load()`` pero mantiene compatibilidad con código
    que antes importaba constantes globales.
    """
    return TeleowsSettings.load()


# ================================================================================
# Funciones adicionales para carga de configuraciones (ETL/Loader)
# ================================================================================

def _replace_env_variables(config: Any) -> Any:
    """
    Reemplaza variables de entorno en el formato ${VAR_NAME} recursivamente.

    Args:
        config: Diccionario, lista o string de configuración

    Returns:
        Configuración con variables reemplazadas
    """
    if isinstance(config, dict):
        return {k: _replace_env_variables(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_replace_env_variables(item) for item in config]
    elif isinstance(config, str):
        # Buscar patrón ${VAR_NAME}
        if config.startswith("${") and config.endswith("}"):
            var_name = config[2:-1]
            value = os.getenv(var_name)
            if value is None:
                import logging
                logging.getLogger(__name__).warning(f"Variable de entorno '{var_name}' no definida")
                return config
            return value
        return config
    else:
        return config


def load_yaml_config(env: Optional[str] = None) -> Dict[str, Any]:
    """
    Carga configuración completa desde config/config_*.yaml con soporte para variables de entorno.
    
    Usa el mismo sistema que otros módulos de energiafacilities (core/utils.py).

    A diferencia de TeleowsSettings (que solo carga campos específicos del scraper),
    esta función retorna TODO el contenido del YAML, útil para loaders que necesitan
    acceder a secciones adicionales como 'postgres', 'gde', etc.

    Args:
        env: Perfil de entorno a cargar (dev, staging, prod)
             Si no se especifica, usa ENV_MODE o 'dev'

    Returns:
        Diccionario con la configuración completa

    Example:
        >>> config = load_yaml_config()
        >>> postgres_config = config.get('postgress', {})
        >>> gde_config = config.get('gde', {})
"""
    import logging
    logger = logging.getLogger(__name__)

    try:
        from envyaml import EnvYAML
    except ImportError:
        raise ImportError("EnvYAML es requerido. Instala con: pip install envyaml")

    try:
        env = env or os.getenv("ENV_MODE", "dev").lower()
        config_path = CONFIG_DIR / f"config_{env}.yaml"

        if not config_path.exists():
            logger.error(f"No existe el archivo de configuración: {config_path}")
            raise FileNotFoundError(f"Archivo no encontrado: {config_path}")

        # Cargar YAML con EnvYAML (hace el reemplazo automático de variables de entorno)
        logger.debug(f"Cargando configuración desde: {config_path}")
        cfg = EnvYAML(config_path, strict=False)
        config = dict(cfg)

        logger.debug(f"Configuración cargada desde perfil '{env}'")
        return config

    except FileNotFoundError as e:
        logger.error(f"No se encontró el archivo: {e}")
        raise
    except Exception as e:
        logger.error(f"Error al cargar configuración: {e}")
        raise
