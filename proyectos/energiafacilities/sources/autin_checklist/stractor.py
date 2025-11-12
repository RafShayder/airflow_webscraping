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
from selenium.webdriver.support.ui import WebDriverWait

from ...clients import (
    AuthManager,
    BrowserManager,
    FilterManager,
    IframeManager,
    DateFilterManager,
    LogManagementManager,
)
from ...common import (
    click_with_retry,
    monitor_export_loader,
    navigate_to_menu_item,
    navigate_to_submenu,
    require,
    wait_for_download,
    wait_for_notification_to_clear,
)
from ...common.dynamic_checklist_constants import (
    MENU_INDEX_DYNAMIC_CHECKLIST,
    MENU_INDEX_LOG_MANAGEMENT,
    SPLITBUTTON_TIMEOUT,
    LOADER_TIMEOUT,
    DOWNLOAD_TIMEOUT,
    DEFAULT_STATUS_POLL_INTERVAL,
    BUTTON_FILTER,
    BUTTON_FILTER_ALT,
    BUTTON_EXPORT,
    BUTTON_REFRESH,
    MENU_DYNAMIC_CHECKLIST,
    MENU_LOG_MANAGEMENT,
    SUBMENU_SUB_PM_QUERY,
    SUBMENU_DATA_EXPORT_LOGS,
    LABEL_SUB_PM_QUERY,
    LABEL_DATA_EXPORT_LOGS,
    CSS_SPLITBUTTON_TEXT,
    XPATH_SPLITBUTTON_BY_TEXT,
    XPATH_PAGINATION_TOTAL,
    XPATH_LOADING_MASK,
    XPATH_SUBMENU_SUB_PM_QUERY,
    XPATH_SUBMENU_DATA_EXPORT_LOGS,
)
from ...config.teleows_config import TeleowsSettings
from ...core.utils import load_settings

logger = logging.getLogger(__name__)

# Alternativas de texto para botones (español/inglés)
BUTTON_ALTERNATIVES = {"Filtrar": "Filter", "Filter": "Filtrar"}


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
        self.status_timeout = status_timeout or settings.max_status_attempts * DEFAULT_STATUS_POLL_INTERVAL
        self.status_poll_interval = status_poll_interval or DEFAULT_STATUS_POLL_INTERVAL
        self.desired_filename = (output_filename or settings.dynamic_checklist_output_filename or "").strip() or None
        self.download_dir = Path(settings.download_path).resolve()
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.run_start = time.time()
        self._overwrite_files = settings.export_overwrite_files

        self.browser_manager = self._setup_browser(headless, chrome_extra_args)
        self.driver, self.wait = self.browser_manager.create_driver()
        self.iframe_manager = IframeManager(self.driver)
        self.filter_manager = FilterManager(self.driver, self.wait)
        self.date_filter_manager = DateFilterManager(self.driver, self.wait)
        # LogManagementManager se inicializa después porque necesita métodos de la instancia
        self.log_management_manager = None

    def run(self) -> Path:
        self.run_start = time.time()
        self._authenticate_and_navigate()
        self._apply_filters()
        return self._export_and_download()

    def _authenticate_and_navigate(self) -> None:
        """Autentica y navega al módulo Dynamic Checklist."""
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
        self._navigate_to_dynamic_checklist()

    def _apply_filters(self) -> None:
        """Aplica los filtros necesarios para la consulta."""
        self._prepare_filters()
        
        # Log de debugging para verificar configuración de fechas
        logger.info("🔍 Configuración de fechas: date_mode=%s, last_n_days=%s, date_from=%s, date_to=%s",
                   getattr(self.settings, 'date_mode', None), getattr(self.settings, 'last_n_days', None),
                   getattr(self.settings, 'date_from', None), getattr(self.settings, 'date_to', None))
        
        self.date_filter_manager.apply_date_filters(self.settings)
        self._click_splitbutton(BUTTON_FILTER)
        self._wait_for_list()

    def _export_and_download(self) -> Path:
        """Ejecuta la exportación y maneja la descarga del archivo."""
        self._click_splitbutton(BUTTON_EXPORT)
        self._wait_for_loader()

        case_detected = monitor_export_loader(self.driver, logger=logger)
        require(case_detected, "No se detectó resultado de exportación antes del timeout")

        if case_detected == "log_management":
            # Inicializar LogManagementManager si aún no está inicializado
            if self.log_management_manager is None:
                self.log_management_manager = LogManagementManager(
                    self.driver,
                    self.wait,
                    self.iframe_manager,
                    self._navigate_to_menu_with_submenu,
                    self._switch_to_last_iframe,
                    self._wait_for_list,
                    self._click_splitbutton,
                    self.status_timeout,
                    self.status_poll_interval,
                )
            self.log_management_manager.handle_log_management()

        downloaded = wait_for_download(
            self.download_dir,
            since=self.run_start,
            overwrite=self._overwrite_files,
            timeout=DOWNLOAD_TIMEOUT,
            desired_name=self.desired_filename,
            logger=logger,
        )
        logger.info("Script completado exitosamente!")
        logger.info("Navegación a Dynamic checklist > Sub PM Query completada")
        logger.info("Filtros aplicados y lista cargada")
        return downloaded

    def close(self) -> None:
        logger.info("ℹ Cerrando navegador...")
        self.browser_manager.close_driver()

    # ========================================================================
    # Configuración inicial
    # ========================================================================

    def _setup_browser(
        self, headless: Optional[bool], chrome_extra_args: Optional[Iterable[str]]
    ) -> BrowserManager:
        """Configura y crea la instancia de BrowserManager con manejo de proxy."""
        browser_kwargs: Dict[str, Any] = {
            "download_path": str(self.download_dir),
            "headless": self.settings.headless if headless is None else headless,
            "extra_args": chrome_extra_args,
        }
        if self.settings.proxy:
            browser_kwargs["proxy"] = self.settings.proxy

        try:
            browser_manager = BrowserManager(**browser_kwargs)
        except TypeError as exc:
            message = str(exc)
            if "unexpected keyword argument 'proxy'" in message and "proxy" in browser_kwargs:
                browser_kwargs.pop("proxy", None)
                logger.warning(
                    " BrowserManager en el entorno no admite 'proxy'. Continuando sin proxy..."
                )
                browser_manager = BrowserManager(**browser_kwargs)
            else:
                raise

        # Configurar proxy si está disponible (código común para ambos casos)
        if not hasattr(browser_manager, "proxy"):
            browser_manager.proxy = self.settings.proxy  # type: ignore[attr-defined]
        if self.settings.proxy:
            os.environ["PROXY"] = self.settings.proxy

        return browser_manager

    # ========================================================================
    # Navegación y configuración inicial
    # ========================================================================

    def _navigate_to_menu_with_submenu(
        self, menu_index: int, menu_title: str, menu_name: str, submenu_xpath: str, submenu_name: str
    ) -> None:
        """
        Navega a un elemento del menú y luego a su submenú, con validación de errores.
        
        Args:
            menu_index: Índice del elemento del menú (basado en cero)
            menu_title: Título del elemento del menú para el selector
            menu_name: Nombre descriptivo del elemento del menú (para logs)
            submenu_xpath: XPath del submenú a seleccionar
            submenu_name: Nombre descriptivo del submenú (para logs)
        """
        require(
            navigate_to_menu_item(
                self.driver,
                self.wait,
                menu_index,
                menu_title,
                menu_name,
                logger=logger,
            ),
            f"No se pudo navegar a {menu_name}",
        )
        require(
            navigate_to_submenu(
                self.wait,
                submenu_xpath,
                submenu_name,
                logger=logger,
            ),
            f"No se pudo abrir {submenu_name}",
        )

    def _navigate_to_dynamic_checklist(self) -> None:
        """Navega al módulo Dynamic Checklist y abre Sub PM Query."""
        navigate_to_menu_item(
            self.driver,
            self.wait,
            MENU_INDEX_DYNAMIC_CHECKLIST,
            MENU_DYNAMIC_CHECKLIST,
            MENU_DYNAMIC_CHECKLIST,
            logger=logger,
        )
        navigate_to_submenu(
            self.wait,
            XPATH_SUBMENU_SUB_PM_QUERY,
            SUBMENU_SUB_PM_QUERY,
            logger=logger,
        )

    # ========================================================================
    # Gestión de iframes
    # ========================================================================

    def _switch_to_last_iframe(self, label: str) -> None:
        """Helper para ubicarse en el último iframe cargado (nuevos contenidos)."""
        require(
            self.iframe_manager.switch_to_last_iframe(),
            f"No se pudo encontrar iframe para {label}",
        )

    # ========================================================================
    # Aplicación de filtros
    # ========================================================================

    def _prepare_filters(self) -> None:
        """Cambia al iframe de Sub PM Query y abre el panel de filtros (método simple)."""
        logger.info("⏳ Esperando a que cargue la sección Sub PM Query...")
        self._switch_to_last_iframe(LABEL_SUB_PM_QUERY)
        self.filter_manager.wait_for_filters_ready()
        logger.info("✅ Sección Sub PM Query cargada correctamente (iframe con filtros activo)")
        logger.info("🔧 Aplicando filtros...")
        require(
            self.filter_manager.open_filter_panel(method="simple"),
            "No se pudo abrir el panel de filtros",
        )


    # ========================================================================
    # Interacciones con la UI (botones, esperas)
    # ========================================================================

    def _find_splitbutton_with_fallback(self, label: str, timeout: int = SPLITBUTTON_TIMEOUT):
        """
        Busca un splitbutton por su texto, con fallback a variantes (español/inglés) y búsqueda alternativa.
        
        Returns:
            WebElement: El elemento del botón encontrado
            
        Raises:
            RuntimeError: Si no se puede encontrar el botón después de todos los intentos
        """
        extended_wait = WebDriverWait(self.driver, timeout)
        
        # Intento 1: Buscar con el texto exacto
        try:
            return extended_wait.until(
                EC.element_to_be_clickable((By.XPATH, XPATH_SPLITBUTTON_BY_TEXT.format(text=label)))
            )
        except TimeoutException:
            pass
        
        # Intento 2: Buscar con texto alternativo (español/inglés)
        if label in BUTTON_ALTERNATIVES:
            alt_label = BUTTON_ALTERNATIVES[label]
            logger.info("Intentando con texto alternativo: '%s'", alt_label)
            try:
                return extended_wait.until(
                    EC.element_to_be_clickable((By.XPATH, XPATH_SPLITBUTTON_BY_TEXT.format(text=alt_label)))
                )
            except TimeoutException:
                pass
        
        # Intento 3: Búsqueda alternativa - buscar todos los splitbuttons y encontrar uno que coincida
        logger.warning("Intentando buscar cualquier splitbutton disponible...")
        buttons = self.driver.find_elements(By.CSS_SELECTOR, CSS_SPLITBUTTON_TEXT)
        if not buttons:
            raise RuntimeError("No se encontraron botones splitbutton disponibles")
        
        logger.info("ℹ Encontrados %s botones splitbutton", len(buttons))
        search_labels = [label]
        if label in BUTTON_ALTERNATIVES:
            search_labels.append(BUTTON_ALTERNATIVES[label])
        
        for btn in buttons:
            btn_text = btn.text.strip()
            logger.info("Botón encontrado: '%s'", btn_text)
            if any(search_label.lower() in btn_text.lower() for search_label in search_labels):
                return btn
        
        raise RuntimeError(f"No se pudo encontrar el botón '{label}' o sus variantes")

    def _click_splitbutton(self, label: str, pause: int = 2) -> None:
        """Hace click en un splitbutton por su texto visible, con fallback automático."""
        button = self._find_splitbutton_with_fallback(label)
        click_with_retry(button, self.driver)
        logger.info("✓ Botón '%s' presionado", label)
        if pause:
            sleep(pause)

    def _wait_for_list(self) -> None:
        """Confirma que la tabla principal esté disponible tras aplicar filtros."""
        logger.info("⏳ Esperando a que cargue la lista...")
        # Aprovechamos el texto "Total" de la paginación para validar que la tabla ya está renderizada.
        total_element = self.wait.until(
            EC.presence_of_element_located((By.XPATH, XPATH_PAGINATION_TOTAL))
        )
        logger.info("✓ Lista cargada: %s", total_element.text)

    # ========================================================================
    # Exportación y monitoreo
    # ========================================================================

    def _wait_for_loader(self) -> None:
        """Detecta el loader posterior a la exportación (asegura que se disparó)."""
        logger.info("⏳ Esperando confirmación de exportación...")
        try:
            self.wait.until(EC.presence_of_element_located((By.XPATH, XPATH_LOADING_MASK)))
            wait_for_notification_to_clear(self.driver, timeout=LOADER_TIMEOUT)
        except TimeoutException:
            logger.warning("⚠ No se detectó loader tras la exportación (posible fallo silencioso)")



# ========================================================================
# Funciones públicas de entrada
# ========================================================================

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
