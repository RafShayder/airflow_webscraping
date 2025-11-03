from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from teleows.config import TeleowsSettings

logger = logging.getLogger(__name__)


def setup_logging(level: str = None) -> None:
    """
    Configura un logger simple hacia stdout. Si no se pasa level, busca en:
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

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=[handler],
    )


def load_settings(overrides: Optional[Dict[str, Any]] = None) -> TeleowsSettings:
    """
    Genera un TeleowsSettings aplicando overrides opcionales.

    Este helper permite que los módulos de ``sources`` utilicen un punto único
    de acceso a la configuración, tal como ocurre con ``load_config`` en
    EnergiaFacilities.
    """
    overrides = overrides or {}
    settings = TeleowsSettings.load_with_overrides(overrides)
    logger.debug("TeleowsSettings generado con overrides: %s", sorted(overrides.keys()))
    return settings


def load_json_config(archivo: str = '', valor: Optional[str] = None) -> Any:
    """
    Carga un archivo JSON desde la carpeta config del proyecto.

    Args:
        archivo: Ruta relativa al archivo JSON desde la raíz del proyecto
        valor: Clave opcional para extraer un valor específico del JSON

    Returns:
        El contenido del JSON completo o el valor específico si se proporciona una clave

    Example:
        >>> load_json_config('config/columnas/columns_map.json', 'gde_tasks')
    """
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / archivo

    if not config_path.exists():
        logger.error(f"No existe el archivo JSON: {config_path}")
        raise FileNotFoundError(f"Archivo no encontrado: {config_path}")

    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            datos = json.load(file)

        if valor:
            if valor not in datos:
                logger.error(f"Clave '{valor}' no encontrada en {archivo}")
                raise KeyError(f"Clave '{valor}' no encontrada en el JSON")
            return datos[valor]
        else:
            return datos
    except json.JSONDecodeError as e:
        logger.error(f"Error al parsear JSON {archivo}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error al cargar JSON {archivo}: {e}")
        raise
