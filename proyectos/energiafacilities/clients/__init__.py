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

__all__ = [
    "AuthManager",
    "BrowserManager",
    "FilterManager",
    "IframeManager",
]
