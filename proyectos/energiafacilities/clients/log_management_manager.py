"""
LogManagementManager: maneja el flujo de Log Management para descargas asíncronas.

Cuando una exportación se ejecuta en segundo plano, este manager navega al módulo
Log Management, monitorea el estado de la exportación y descarga el archivo cuando
está listo.
"""

from __future__ import annotations

import logging
import time
from time import sleep
from typing import Any, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from ..common.dynamic_checklist_constants import (
    MENU_INDEX_LOG_MANAGEMENT,
    MENU_LOG_MANAGEMENT,
    SUBMENU_DATA_EXPORT_LOGS,
    LABEL_DATA_EXPORT_LOGS,
    XPATH_SUBMENU_DATA_EXPORT_LOGS,
    XPATH_EXPORT_ROW,
    XPATH_EXPORT_STATUS_CELL,
    XPATH_DOWNLOAD_BUTTON,
    XPATH_CLOSE_PROMPT_BUTTON,
    EXPORT_END_STATES,
    EXPORT_SUCCESS_STATE,
    EXPORT_RUNNING_STATE,
    SLEEP_AFTER_PROMPT_CLOSE,
    SLEEP_AFTER_REFRESH,
    SLEEP_AFTER_DOWNLOAD_CLICK,
)

logger = logging.getLogger(__name__)


class LogManagementManager:
    """Maneja el flujo de Log Management para descargas asíncronas."""

    def __init__(
        self, 
        driver: WebDriver, 
        wait: WebDriverWait,
        iframe_manager: Any,
        navigate_to_menu_with_submenu: Any,
        switch_to_last_iframe: Any,
        wait_for_list: Any,
        click_splitbutton: Any,
        status_timeout: int = 300,
        status_poll_interval: int = 30,
    ):
        """
        Inicializa el manager de Log Management.
        
        Args:
            driver: Instancia de WebDriver
            wait: Instancia de WebDriverWait
            iframe_manager: Manager de iframes para cambiar contexto
            navigate_to_menu_with_submenu: Función para navegar a menús
            switch_to_last_iframe: Función para cambiar a último iframe
            wait_for_list: Función para esperar carga de lista
            click_splitbutton: Función para hacer clic en splitbuttons
            status_timeout: Timeout máximo para monitorear estado (segundos)
            status_poll_interval: Intervalo entre verificaciones de estado (segundos)
        """
        self.driver = driver
        self.wait = wait
        self.iframe_manager = iframe_manager
        self.navigate_to_menu_with_submenu = navigate_to_menu_with_submenu
        self.switch_to_last_iframe = switch_to_last_iframe
        self.wait_for_list = wait_for_list
        self.click_splitbutton = click_splitbutton
        self.status_timeout = status_timeout
        self.status_poll_interval = status_poll_interval

    def handle_log_management(self) -> None:
        """Sigue el flujo de Log Management cuando la exportación corre en segundo plano."""
        self.close_export_prompt()
        self.iframe_manager.switch_to_default_content()
        logger.info("📋 Navegando a Log Management...")
        self.navigate_to_menu_with_submenu(
            menu_index=MENU_INDEX_LOG_MANAGEMENT,
            menu_title=MENU_LOG_MANAGEMENT,
            menu_name=MENU_LOG_MANAGEMENT,
            submenu_xpath=XPATH_SUBMENU_DATA_EXPORT_LOGS,
            submenu_name=SUBMENU_DATA_EXPORT_LOGS,
        )
        logger.info("⏳ Cambiando al iframe de Data Export Logs...")
        self.switch_to_last_iframe(LABEL_DATA_EXPORT_LOGS)
        self.wait_for_list()
        # Una vez en la tabla de logs, monitorizamos el estado hasta poder descargar.
        self.monitor_log_management()

    def close_export_prompt(self) -> None:
        """Intenta cerrar el modal de advertencia antes de cambiar de módulo."""
        try:
            close_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, XPATH_CLOSE_PROMPT_BUTTON))
            )
            close_button.click()
            logger.info("✓ Mensaje de información cerrado")
            sleep(SLEEP_AFTER_PROMPT_CLOSE)
        except Exception:
            # El cierre no es crítico, pero dejamos registro si falla.
            logger.warning("⚠ No se pudo cerrar el mensaje", exc_info=True)

    def monitor_log_management(self) -> None:
        """Revisa repetidamente el estado de la exportación y dispara la descarga final."""
        logger.info("🔍 Buscando exportación en progreso...")
        deadline = time.time() + self.status_timeout
        attempt = 0

        # Consultamos la tabla hasta obtener un estado terminal o agotar el timeout configurado.
        while time.time() < deadline:
            attempt += 1
            try:
                self.refresh_export_status()
                target_row = self.find_export_row()
                status = self.get_export_status(target_row)
                
                logger.info(
                    "📊 Status de exportación: %s (intento %s, quedan %.0f s)",
                    status,
                    attempt,
                    max(0, deadline - time.time()),
                )

                if status in EXPORT_END_STATES:
                    if status == EXPORT_SUCCESS_STATE:
                        self.download_from_log_table(target_row)
                        return
                    raise RuntimeError(f"Proceso de exportación terminó con estado: {status}")

                if status != EXPORT_RUNNING_STATE:
                    logger.warning("⚠ Status inesperado: %s", status)

            except RuntimeError:
                raise
            except Exception:
                logger.exception("❌ Error al revisar exportación (intento %s)", attempt)

            sleep(self.status_poll_interval)

        message = "Tiempo máximo de espera alcanzado para la exportación"
        logger.error("⏱️ %s", message)
        raise RuntimeError(message)

    def refresh_export_status(self) -> None:
        """Refresca el estado de la exportación en la tabla."""
        try:
            from ..common.dynamic_checklist_constants import BUTTON_REFRESH
            self.click_splitbutton(BUTTON_REFRESH, pause=0)
        except Exception:
            logger.warning("⚠ Error al presionar Refresh", exc_info=True)
        sleep(SLEEP_AFTER_REFRESH)

    def find_export_row(self) -> WebElement:
        """Encuentra la fila de la tabla correspondiente a la exportación de Dynamic Checklist."""
        return self.driver.find_element(By.XPATH, XPATH_EXPORT_ROW)

    def get_export_status(self, target_row: WebElement) -> str:
        """Obtiene el estado de exportación desde la fila de la tabla."""
        return target_row.find_element(By.XPATH, XPATH_EXPORT_STATUS_CELL).text.strip()

    def download_from_log_table(self, target_row: WebElement) -> None:
        """Descarga el archivo desde la tabla de logs cuando la exportación está completa."""
        logger.info("✅ Exportación completada exitosamente!")
        download_button = target_row.find_element(By.XPATH, XPATH_DOWNLOAD_BUTTON)
        download_button.click()
        logger.info("✓ Click en 'Download' - archivo descargándose...")
        sleep(SLEEP_AFTER_DOWNLOAD_CLICK)

