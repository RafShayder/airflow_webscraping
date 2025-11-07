"""
IframeManager: helper para localizar y cambiar entre iframes en la UI de Integratel.

Reubicado desde ``teleows.core.iframe_manager`` para que las dependencias de Selenium
residan en ``teleows.clients``.
"""

import logging
from time import sleep

from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)


class IframeManager:
    """Centraliza la lógica de cambio de iframes en el portal."""

    def __init__(self, driver) -> None:
        self.driver = driver

    def find_main_iframe(self, max_attempts: int = 60, selector: str = ".ows_filter_title") -> bool:
        """Localiza el iframe que contiene el selector clave y cambia el contexto."""
        logger.info("⏳ Esperando a que cargue el iframe con filtros...")
        iframe_encontrado = False
        intentos = 0

        while not iframe_encontrado and intentos < max_attempts:
            try:
                iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                for frame in iframes:
                    self.driver.switch_to.frame(frame)
                    if self.driver.execute_script(f"return !!document.querySelector('{selector}');"):
                        logger.info("✓ Iframe principal encontrado.")
                        iframe_encontrado = True
                        break
                    self.driver.switch_to.default_content()

                if not iframe_encontrado:
                    intentos += 1
                    logger.info("Intento %s/%s - Esperando carga del iframe...", intentos, max_attempts)
                    sleep(2)
            except Exception as exc:
                logger.warning("⚠ Error en intento %s: %s", intentos, exc, exc_info=True)
                self.driver.switch_to.default_content()
                intentos += 1
                sleep(2)

        if not iframe_encontrado:
            message = "No se encontró el iframe principal después de esperar."
            logger.error("❌ %s", message)
            raise RuntimeError(message)

        logger.info("✅ Iframe cargado correctamente.")
        return True

    def switch_to_iframe(self, index: int) -> bool:
        """Cambio directo a un iframe por índice (utilizado tras contar iframes)."""
        try:
            self.driver.switch_to.frame(index)
            logger.info("✓ Cambiado a iframe index=%s", index)
            return True
        except Exception as exc:
            message = f"Error al cambiar a iframe {index}: {exc}"
            logger.error("❌ %s", message, exc_info=True)
            raise RuntimeError(message) from exc

    def switch_to_default_content(self) -> bool:
        """Regresa al documento principal (fuera de todos los iframes)."""
        try:
            self.driver.switch_to.default_content()
            logger.info("✓ Cambiado al contenido principal")
            return True
        except Exception as exc:
            message = f"Error al cambiar al contenido principal: {exc}"
            logger.error("❌ %s", message, exc_info=True)
            raise RuntimeError(message) from exc

    def get_iframe_count(self) -> int:
        """Devuelve la cantidad de iframes presentes en el documento actual."""
        try:
            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            count = len(iframes)
            logger.info("ℹ Total de iframes encontrados: %s", count)
            return count
        except Exception as exc:
            message = f"Error al contar iframes: {exc}"
            logger.error("❌ %s", message, exc_info=True)
            raise RuntimeError(message) from exc
