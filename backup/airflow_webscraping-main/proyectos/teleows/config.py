import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

BASE_DIR = Path(__file__).resolve().parent
SETTINGS_PATH = BASE_DIR / "settings.yaml"

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
}

_TRUE_VALUES = {"1", "true", "yes", "on"}


def _default_download_path() -> str:
    if Path("/opt/airflow").exists():
        return "/opt/airflow/proyectos/teleows/temp"
    return str(Path.home() / "Downloads" / "scraper_downloads")


def _load_settings_file() -> Dict[str, Any]:
    if not SETTINGS_PATH.exists():
        return {}

    try:
        import yaml  # type: ignore
    except ImportError:  # pragma: no cover
        # Si PyYAML no está disponible simplemente ignoramos el archivo y
        # continuamos con los valores de entorno.
        return {}

    with SETTINGS_PATH.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    if not isinstance(raw, dict):
        raise RuntimeError("El archivo settings.yaml debe contener un mapeo.")

    profile = os.getenv("TELEOWS_ENV", "default")
    section = raw.get(profile, raw)
    if not isinstance(section, dict):
        raise RuntimeError(
            f"La sección '{profile}' de settings.yaml debe ser un objeto con pares clave/valor."
        )

    return dict(section)


def _load_env_overrides() -> Dict[str, Any]:
    overrides: Dict[str, Any] = {}
    for field, env_var in _ENV_FIELD_MAP.items():
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

    @classmethod
    def load(cls) -> "TeleowsSettings":
        raw_settings: Dict[str, Any] = {}
        raw_settings.update(_load_settings_file())
        raw_settings.update(_load_env_overrides())

        username = _as_optional_str(raw_settings.get("username"))
        password = _as_optional_str(raw_settings.get("password"))

        if not username or not password:
            raise ValueError(
                "Credenciales no configuradas. Define USERNAME y PASSWORD (ya sea en variables "
                "de entorno, Airflow o settings.yaml)."
            )

        download_path_setting = (
            _as_optional_str(raw_settings.get("download_path")) or _default_download_path()
        )
        download_path = Path(download_path_setting).expanduser()
        try:
            download_path.mkdir(parents=True, exist_ok=True)
        except OSError:
            # No interrumpir si no es posible crear el directorio
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

        return cls(
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
        )


SETTINGS = TeleowsSettings.load()

USERNAME = SETTINGS.username
PASSWORD = SETTINGS.password
DOWNLOAD_PATH = SETTINGS.download_path
MAX_IFRAME_ATTEMPTS = SETTINGS.max_iframe_attempts
MAX_STATUS_ATTEMPTS = SETTINGS.max_status_attempts
OPTIONS_TO_SELECT = list(SETTINGS.options_to_select)
DATE_MODE = SETTINGS.date_mode
DATE_FROM = SETTINGS.date_from
DATE_TO = SETTINGS.date_to
GDE_OUTPUT_FILENAME = SETTINGS.gde_output_filename
DYNAMIC_CHECKLIST_OUTPUT_FILENAME = SETTINGS.dynamic_checklist_output_filename
EXPORT_OVERWRITE_FILES = SETTINGS.export_overwrite_files
PROXY = SETTINGS.proxy

if PROXY and not os.getenv("PROXY"):
    # Mantener compatibilidad con componentes que leen directamente os.environ.
    os.environ["PROXY"] = PROXY
