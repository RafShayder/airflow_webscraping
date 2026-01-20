"""
Clientes (objetos de alto nivel) que encapsulan interacciones con Selenium:

- AuthManager: login y gestión de sesión (soporta Integratel y portales genéricos).
- LoginSelectors: dataclass para configurar selectores de login.
- BrowserManager: configuración de Chrome/Chromium con rutas de descarga y banderas.
- FilterManager: utilidades para abrir paneles de filtro.
- IframeManager: cambios de contexto entre iframes.

Se reexportan aquí para ofrecer una API estable al resto del paquete.
"""

from .auth import AuthManager, LoginSelectors
from .browser import BrowserManager, setup_browser_with_proxy
from .filters import FilterManager
from .iframes import IframeManager
from .date_filter_manager import DateFilterManager
from .log_management_manager import LogManagementManager

__all__ = [
    "AuthManager",
    "LoginSelectors",
    "BrowserManager",
    "setup_browser_with_proxy",
    "FilterManager",
    "IframeManager",
    "DateFilterManager",
    "LogManagementManager",
]
