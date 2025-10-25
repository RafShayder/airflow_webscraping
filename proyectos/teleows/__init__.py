"""
Este paquete contiene módulos para automatizar la descarga de reportes
desde la plataforma.
"""

__version__ = "1.0.0"
__author__ = "Scraper Team"

from .GDE import run_gde  # noqa: E402
from .dynamic_checklist import run_dynamic_checklist  # noqa: E402

__all__ = ["run_gde", "run_dynamic_checklist"]
