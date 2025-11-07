"""
FilterManager: sincroniza la apertura del panel de filtros en los distintos iframes.

Proviene de ``teleows.core.filter_manager``. Se mueve a ``teleows.clients`` para
mantener una separación clara entre clientes (objetos que operan sobre el driver)
y los workflows de scraping.
"""

import logging
from time import sleep

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)


class FilterManager:
    """Maneja esperas y aperturas de paneles de filtrado en el portal."""

    def __init__(self, driver, wait):
        self.driver = driver
        self.wait = wait

    def wait_for_filters_ready(self) -> None:
        """Sincroniza hasta que los botones de filtro de la vista estén disponibles."""
        try:
            logger.info("⏳ Esperando a que la página esté lista para filtros...")
            self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "v-icon-o-filter")))
            logger.info("✓ Botones de filtro están disponibles")
            try:
                self.driver.find_element(By.XPATH, "//*[contains(text(),'Opciones de filtrado')]")
                logger.info("✓ 'Opciones de filtrado' está disponible")
            except Exception:
                logger.warning(
                    "⚠ Texto 'Opciones de filtrado' no encontrado, pero el ícono de filtro sí está presente"
                )
            logger.info("✅ Página lista para aplicar filtros")
        except Exception as exc:
            message = f"Error al preparar filtros: {exc}"
            logger.error("❌ %s", message, exc_info=True)
            try:
                import time
                screenshot_path = f"/app/temp/error_screenshot_{int(time.time())}.png"
                self.driver.save_screenshot(screenshot_path)
                logger.info("📸 Screenshot guardado en: %s", screenshot_path)
            except Exception:
                pass
            raise RuntimeError(message) from exc

    def open_filter_panel(self, method: str = "simple") -> bool:
        """Abre el panel de filtros siguiendo la estrategia solicitada."""
        if method == "simple":
            return self._open_filter_panel_simple()
        if method == "complex":
            return self._open_filter_panel_complex()
        message = f"Método desconocido para abrir filtros: {method}"
        logger.error("❌ %s", message)
        raise ValueError(message)

    def _open_filter_panel_simple(self) -> bool:
        """Abre el panel haciendo click directo sobre el icono visible."""
        try:
            filter_icon = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "i.vigour-iconfont.v-icon-o-filter"))
            )
            filter_icon.click()
            logger.info("✓ Panel de filtros abierto (método simple)")
            sleep(2)
            return True
        except Exception as exc:
            message = f"Error al abrir panel de filtros (simple): {exc}"
            logger.error("❌ %s", message, exc_info=True)
            raise RuntimeError(message) from exc

    def _open_filter_panel_complex(self) -> bool:
        """Recorre iframes y hace click mediante JavaScript cuando es necesario."""
        try:
            script_check = """
                const elements = document.getElementsByClassName("v-icon-o-filter");
                return elements.length;
            """
            num_elementos = self.driver.execute_script(script_check)
            logger.info("ℹ Elementos 'v-icon-o-filter' encontrados: %s", num_elementos)

            if num_elementos >= 5:
                self.driver.execute_script(
                    'document.getElementsByClassName("v-icon-o-filter")[4].click();'
                )
                logger.info("✓ Panel de filtros abierto (índice 4)")
                sleep(1)
                return True

            if num_elementos > 0:
                index = num_elementos - 1
                self.driver.execute_script(
                    f'document.getElementsByClassName("v-icon-o-filter")[{index}].click();'
                )
                logger.info("✓ Panel de filtros abierto (índice %s)", index)
                sleep(1)
                return True

            logger.warning("⚠ Elemento no encontrado en contexto actual, buscando en iframes...")
            self.driver.switch_to.default_content()
            frames = self.driver.find_elements(By.TAG_NAME, "iframe")
            logger.info("ℹ Total de iframes: %s", len(frames))

            for idx, frame in enumerate(frames):
                self.driver.switch_to.frame(frame)
                num_elementos = self.driver.execute_script(script_check)
                logger.info("  Iframe %s: %s elementos", idx, num_elementos)

                if num_elementos > 0:
                    index_to_use = min(4, num_elementos - 1)
                    self.driver.execute_script(
                        f'document.getElementsByClassName("v-icon-o-filter")[{index_to_use}].click();'
                    )
                    logger.info("✓ Panel de filtros abierto en iframe %s (índice %s)", idx, index_to_use)
                    sleep(1)
                    return True
                self.driver.switch_to.default_content()

            message = "No se pudo abrir el panel de filtros en ningún iframe"
            logger.error("❌ %s", message)
            raise RuntimeError(message)

        except Exception as exc:
            message = f"Error al abrir panel de filtros (complex): {exc}"
            logger.error("❌ %s", message, exc_info=True)
            raise RuntimeError(message) from exc
