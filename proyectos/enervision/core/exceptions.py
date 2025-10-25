from __future__ import annotations

class ETLError(Exception):
    """Excepción base para toda la capa ETL."""
    pass

class CredentialsError(ETLError):
    """Error al obtener credenciales (Vault / env)."""
    pass

class ConfigError(ETLError):
    """Error en la configuración (yaml, claves faltantes, etc.)."""
    pass

class ExtractError(ETLError):
    """Error durante la extracción de datos."""
    pass

class RetryableExtractError(ExtractError):
    """Error de extracción potencialmente transitorio (reintentar)."""
    pass

class NonRetryableExtractError(ExtractError):
    """Error de extracción definitivo (no reintentar)."""
    pass

class TransformError(ETLError):
    """Error durante la transformación."""
    pass

class LoadError(ETLError):
    """Error durante la carga a destino."""
    pass
