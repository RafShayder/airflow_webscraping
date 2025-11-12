"""
Pipeline GDE para Teleows.

Los helpers definidos aquí actúan como punto de integración entre los
workflows de Selenium existentes y la futura capa de ingestión/transformación.
"""

from .stractor import extraer_gde, run_gde  # noqa: F401
# from .transformer import transformar_gde  # noqa: F401  # No existe: transformación se hace en BD con SP
from .loader import load_gde  # noqa: F401
# from .run_sp import correr_sp_gde  # noqa: F401  # No existe: solo extract y load

__all__ = [
    "extraer_gde",
    "run_gde",
    # "transformar_gde",  # No existe: transformación se hace en BD con SP
    "load_gde",
    # "correr_sp_gde",  # No existe: solo extract y load
]
