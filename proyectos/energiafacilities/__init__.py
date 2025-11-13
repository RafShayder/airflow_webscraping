"""
Paquete energiafacilities: herramientas para automatizar reportes y scraping en Integratel.

API pública:
    - GDEConfig, DynamicChecklistConfig: configuración desde config YAML.
    - extraer_gde, extraer_dynamic_checklist: funciones de extracción principales.
    - run_gde, run_dynamic_checklist: workflows internos (llamados por extraer_*).
    - setup_logging: configuración de logging.
"""

from energiafacilities.core import setup_logging
from energiafacilities.sources.autin_gde.stractor import GDEConfig, extraer_gde, run_gde
from energiafacilities.sources.autin_checklist.stractor import DynamicChecklistConfig, extraer_dynamic_checklist, run_dynamic_checklist

__all__ = [
    "GDEConfig",
    "DynamicChecklistConfig",
    "run_gde",
    "run_dynamic_checklist",
    "extraer_gde",
    "extraer_dynamic_checklist",
    "setup_logging",
]
