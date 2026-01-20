"""
Paquete energiafacilities: herramientas para automatizar reportes y scraping en Integratel.

API pública:
    - setup_logging: configuración de logging.
    
Nota: Los DAGs importan directamente desde los módulos fuente:
    - from energiafacilities.sources.autin_gde.stractor import GDEConfig, extraer_gde
    - from energiafacilities.sources.autin_checklist.stractor import DynamicChecklistConfig, extraer_dynamic_checklist
"""

from .core import setup_logging

__all__ = [
    "setup_logging",
]
