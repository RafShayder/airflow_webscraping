"""
Clientes (objetos de alto nivel) que encapsulan interacciones con Selenium:

- AuthManager: login y gestión de sesión en el portal Integratel.
- BrowserManager: configuración de Chrome/Chromium con rutas de descarga y banderas.
- FilterManager: utilidades para abrir paneles de filtro.
- IframeManager: cambios de contexto entre iframes.

Se reexportan aquí para ofrecer una API estable al resto del paquete.
"""

from clients.auth import AuthManager
from clients.browser import BrowserManager
from clients.filters import FilterManager
from clients.iframes import IframeManager
from clients.date_filter_manager import DateFilterManager
from clients.log_management_manager import LogManagementManager

__all__ = [
    "AuthManager",
    "BrowserManager",
    "FilterManager",
    "IframeManager",
    "DateFilterManager",
    "LogManagementManager",
]
