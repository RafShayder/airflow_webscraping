"""
Workflow Dynamic Checklist: automatiza la exportación del reporte
``Dynamic checklist > Sub PM Query``.

=== Flujo general ===
1) Configuración / Navegador:
   - ``TeleowsSettings`` agrega credenciales, fechas, rutas de descarga, etc.
   - ``BrowserManager`` (teleows.clients.browser) genera el driver con proxy,
     headless y ruta de descargas.
2) Login y contexto:
   - ``AuthManager`` inicia sesión.
   - ``IframeManager`` ubica el iframe principal y posteriores cambios.
   - ``FilterManager`` abre el panel de filtros (método simple).
3) Aplicación de filtros:
   - Helpers de este archivo gestionan selección de fechas/tipo y clicks
     (``_select_last_month``, ``_click_splitbutton``, etc.).
4) Exportación:
   - ``_click_splitbutton('Export sub WO detail')`` lanza la exportación.
   - ``_wait_for_loader`` verifica que el proceso haya iniciado.
   - ``monitor_export_loader`` (teleows.common) decide si hay descarga directa
     o ruta vía Log Management.
5) Descarga:
   - Descarga directa: ``wait_for_download`` ubica el archivo y lo renombra.
   - Log Management: ``_handle_log_management`` navega al módulo, refresca la
     tabla y hace click en Download, reutilizando ``wait_for_download``.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, Optional

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from ...clients import AuthManager, BrowserManager, FilterManager, IframeManager
from ...common import (
    click_with_retry,
    monitor_export_loader,
    navigate_to_menu_item,
    navigate_to_submenu,
    require,
    wait_for_download,
    wait_for_notification_to_clear,
)
from ...config import TeleowsSettings
from ...core.utils import load_settings

logger = logging.getLogger(__name__)


class DynamicChecklistWorkflow:
    """
    Implementa el flujo completo de Dynamic Checklist.

    Componentes clave utilizados:
        - ``BrowserManager``: instancia del driver de Selenium con rutas de descarga.
        - ``AuthManager``: login al portal Integratel.
        - ``IframeManager``: cambio entre iframes para acceder a Sub PM Query y Log Management.
        - ``FilterManager``: apertura/esperas del panel de filtros.
        - Helpers locales: encapsulan interacciones específicas de la UI (split button,
          selección de fechas, espera de la tabla ag-grid, etc.).
    """

    def __init__(
        self,
        settings: TeleowsSettings,
        headless: Optional[bool] = None,
        chrome_extra_args: Optional[Iterable[str]] = None,
        status_timeout: Optional[int] = None,
        status_poll_interval: Optional[int] = None,
        output_filename: Optional[str] = None,
    ) -> None:
        self.settings = settings
        self.status_timeout = status_timeout or settings.max_status_attempts * 30
        self.status_poll_interval = status_poll_interval or 30
        self.desired_filename = (output_filename or settings.dynamic_checklist_output_filename or "").strip() or None
        self.download_dir = Path(settings.download_path).resolve()
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.run_start = time.time()
        self._overwrite_files = settings.export_overwrite_files

        browser_kwargs: Dict[str, Any] = {
            "download_path": str(self.download_dir),
            "headless": settings.headless if headless is None else headless,
            "extra_args": chrome_extra_args,
        }
        if settings.proxy:
            browser_kwargs["proxy"] = settings.proxy

        try:
            self.browser_manager = BrowserManager(**browser_kwargs)
        except TypeError as exc:
            message = str(exc)
            if "unexpected keyword argument 'proxy'" in message and "proxy" in browser_kwargs:
                browser_kwargs.pop("proxy", None)
                logger.warning(
                    "⚠ BrowserManager en el entorno no admite 'proxy'. Continuando sin proxy..."
                )
                self.browser_manager = BrowserManager(**browser_kwargs)
                if not hasattr(self.browser_manager, "proxy"):
                    self.browser_manager.proxy = self.settings.proxy  # type: ignore[attr-defined]
                if self.settings.proxy:
                    os.environ["PROXY"] = self.settings.proxy
            else:
                raise
        else:
            if not hasattr(self.browser_manager, "proxy"):
                self.browser_manager.proxy = self.settings.proxy  # type: ignore[attr-defined]
            if self.settings.proxy:
                os.environ["PROXY"] = self.settings.proxy

        self.driver, self.wait = self.browser_manager.create_driver()
        self.iframe_manager = IframeManager(self.driver)
        self.filter_manager = FilterManager(self.driver, self.wait)

    def run(self) -> Path:
        self.run_start = time.time()

        auth_manager = AuthManager(self.driver)
        require(
            auth_manager.login(self.settings.username, self.settings.password),
            "No se pudo realizar el login.",
        )

        require(
            self.iframe_manager.find_main_iframe(max_attempts=self.settings.max_iframe_attempts),
            "No se encontró el iframe principal",
        )
        self.iframe_manager.switch_to_default_content()
        self._open_dynamic_checklist()
        self._open_sub_pm_query()
        self._prepare_filters()
        self._select_last_month()
        self._click_splitbutton("Filter")
        self._wait_for_list()

        self._click_splitbutton("Export sub WO detail")
        self._wait_for_loader()

        case_detected = monitor_export_loader(self.driver, logger=logger)
        require(case_detected, "No se detectó resultado de exportación antes del timeout")

        if case_detected == "log_management":
            self._handle_log_management()

        downloaded = wait_for_download(
            self.download_dir,
            since=self.run_start,
            overwrite=self._overwrite_files,
            timeout=300,
            desired_name=self.desired_filename,
            logger=logger,
        )
        logger.info("🎉 Script completado exitosamente!")
        logger.info("📋 Navegación a Dynamic checklist > Sub PM Query completada")
        logger.info("🔧 Filtros aplicados y lista cargada")
        return downloaded

    def close(self) -> None:
        logger.info("ℹ Cerrando navegador...")
        self.browser_manager.close_driver()

    # --- Pasos del flujo -------------------------------------------------

    def _open_dynamic_checklist(self) -> None:
        """Posiciona el menú lateral en "Dynamic checklist" (helper común)."""
        navigate_to_menu_item(self.driver, self.wait, 5, "Dynamic checklist", "Dynamic checklist", logger=logger)

    def _open_sub_pm_query(self) -> None:
        """Selecciona el submenú "Sub PM Query" dentro de Dynamic checklist."""
        navigate_to_submenu(
            self.wait,
            "//span[@class='level-1 link-nav' and @title='Sub PM Query']",
            "Sub PM Query",
            logger=logger,
        )

    def _prepare_filters(self) -> None:
        """Cambia al iframe de Sub PM Query y abre el panel de filtros (método simple)."""
        logger.info("⏳ Esperando a que cargue la sección Sub PM Query...")
        self._switch_to_last_iframe("Sub PM Query")
        self.filter_manager.wait_for_filters_ready()
        logger.info("✅ Sección Sub PM Query cargada correctamente (iframe con filtros activo)")
        logger.info("🔧 Aplicando filtros...")
        require(
            self.filter_manager.open_filter_panel(method="simple"),
            "No se pudo abrir el panel de filtros",
        )

    def _switch_to_last_iframe(self, label: str) -> None:
        """Helper para ubicarse en el último iframe cargado (nuevos contenidos)."""
        require(
            self.iframe_manager.switch_to_last_iframe(),
            f"No se pudo encontrar iframe para {label}",
        )

    def _select_last_month(self) -> None:
        """Selecciona el rango de fechas 'Last month'."""
        logger.info("🗓 Seleccionando rango 'Last month' en filtros")
        last_month_btn = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//span[text()='Last month']"))
        )
        click_with_retry(last_month_btn, self.driver)
        sleep(1)

    def _click_splitbutton(self, option: str) -> None:
        """Hace click en cualquiera de las opciones del split button principal."""
        logger.info("🖱 Click en split button: %s", option)
        button = self.wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//div[contains(@class,'sdm_splitbutton_items')]//span[text()='{option}']")
            )
        )
        click_with_retry(button, self.driver)
        sleep(1)

    def _wait_for_list(self) -> None:
        """Espera a que la tabla principal cargue resultados."""
        logger.info("⏳ Esperando resultados en la lista principal...")
        try:
            self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'ag-root-wrapper')]"))
            )
            logger.info("✅ Lista cargada correctamente")
        except TimeoutException as exc:
            raise RuntimeError("La lista principal nunca terminó de cargar.") from exc

    def _wait_for_loader(self) -> None:
        """Detecta el loader posterior a la exportación (asegura que se disparó)."""
        logger.info("⏳ Esperando confirmación de exportación...")
        try:
            self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'el-loading-mask')]"))
            )
            wait_for_notification_to_clear(self.driver, timeout=60, logger=logger)
        except TimeoutException:
            logger.warning("⚠ No se detectó loader tras la exportación (posible fallo silencioso)")

    def _handle_log_management(self) -> None:
        """Navega al módulo Log Management para descargar desde el historial."""
        logger.info("📚 Exportación derivada a Log Management. Navegando...")
        self.iframe_manager.switch_to_default_content()
        navigate_to_menu_item(
            self.driver,
            self.wait,
            5,
            "Log Management",
            "Log Management",
            logger=logger,
        )
        self._switch_to_last_iframe("Log Management")

        refresh_btn = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//i[contains(@class,'el-icon-refresh')]"))
        )
        click_with_retry(refresh_btn, self.driver)
        sleep(2)

        download_btn = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//span[text()='Download']/ancestor::button"))
        )
        click_with_retry(download_btn, self.driver)
        wait_for_notification_to_clear(self.driver, timeout=120, logger=logger)


def run_dynamic_checklist(
    settings: TeleowsSettings,
    *,
    headless: Optional[bool] = None,
    chrome_extra_args: Optional[Iterable[str]] = None,
    status_timeout: Optional[int] = None,
    status_poll_interval: Optional[int] = None,
    output_filename: Optional[str] = None,
) -> Path:
    """
    Ejecuta el flujo completo de Dynamic Checklist y devuelve la ruta del archivo descargado.
    """
    workflow = DynamicChecklistWorkflow(
        settings=settings,
        headless=headless,
        chrome_extra_args=chrome_extra_args,
        status_timeout=status_timeout,
        status_poll_interval=status_poll_interval,
        output_filename=output_filename,
    )
    try:
        return workflow.run()
    finally:
        workflow.close()


def extraer_dynamic_checklist(
    settings: Optional[TeleowsSettings] = None,
    *,
    overrides: Optional[Dict[str, Any]] = None,
    headless: Optional[bool] = None,
    chrome_extra_args: Optional[Iterable[str]] = None,
    status_timeout: Optional[int] = None,
    status_poll_interval: Optional[int] = None,
    output_filename: Optional[str] = None,
) -> str:
    """
    Ejecuta el workflow Dynamic Checklist y devuelve la ruta del archivo exportado.
    """
    effective_settings = settings or load_settings(overrides)
    logger.info(
        "Iniciando extracción Dynamic Checklist (overrides=%s)",
        sorted(overrides.keys()) if overrides else "default",
    )
    path = run_dynamic_checklist(
        effective_settings,
        headless=headless,
        chrome_extra_args=chrome_extra_args,
        status_timeout=status_timeout,
        status_poll_interval=status_poll_interval,
        output_filename=output_filename,
    )
    logger.info("Extracción Dynamic Checklist finalizada. Archivo: %s", path)
    return str(path)
