"""
Clientes (objetos de alto nivel) que encapsulan interacciones con Selenium:

- AuthManager: login y gestión de sesión en el portal Integratel.
- BrowserManager: configuración de Chrome/Chromium con rutas de descarga y banderas.
- FilterManager: utilidades para abrir paneles de filtro.
- IframeManager: cambios de contexto entre iframes.

Se reexportan aquí para ofrecer una API estable al resto del paquete.
"""

from .auth import AuthManager
from .browser import BrowserManager
from .filters import FilterManager
from .iframes import IframeManager
from .date_filter_manager import DateFilterManager
from .log_management_manager import LogManagementManager

__all__ = [
    "AuthManager",
    "BrowserManager",
    "FilterManager",
    "IframeManager",
    "DateFilterManager",
    "LogManagementManager",
]
