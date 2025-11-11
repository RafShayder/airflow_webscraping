"""
Paquete energiafacilities: herramientas para automatizar reportes y scraping en Integratel.

API pública:
    - TeleowsSettings: objeto de configuración unificado (teleows_config.py).
    - run_gde, run_dynamic_checklist: workflows listos para Airflow/scripts.
    - extraer_gde, extraer_dynamic_checklist: funciones de extracción principales.
"""

from .teleows_config import TeleowsSettings
from .core import load_settings, setup_logging
from .sources.gde.stractor import extraer_gde, run_gde
from .sources.dynamic_checklist.stractor import extraer_dynamic_checklist, run_dynamic_checklist

__all__ = [
    "run_gde",
    "run_dynamic_checklist",
    "extraer_gde",
    "extraer_dynamic_checklist",
    "TeleowsSettings",
    "load_settings",
    "setup_logging",
]
