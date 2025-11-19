"""
Workflow Dynamic Checklist: automatiza la exportación del reporte
``Dynamic checklist > Sub PM Query``.

=== Flujo general ===
1) Configuración / Navegador:
   - ``DynamicChecklistConfig`` carga credenciales, fechas, rutas desde config YAML.
   - ``BrowserManager`` (clients.browser) genera el driver con proxy,
     headless y ruta de descargas.
2) Login y contexto:
   - ``AuthManager`` inicia sesión.
   - ``IframeManager`` ubica el iframe principal y posteriores cambios.
   - ``FilterManager`` abre el panel de filtros (método simple).
3) Aplicación de filtros:
   - ``DateFilterManager`` aplica filtros de fecha (unificado con GDE).
   - Helpers de este archivo gestionan clicks en splitbuttons.
4) Exportación:
   - ``_click_splitbutton('Export sub WO detail')`` lanza la exportación.
   - ``_wait_for_loader`` verifica que el proceso haya iniciado.
   - ``monitor_export_loader`` decide si hay descarga directa
     o ruta vía Log Management.
5) Descarga:
   - Descarga directa: ``wait_for_download`` ubica el archivo y lo renombra.
   - Log Management: ``LogManagementManager`` navega al módulo, refresca la
     tabla y hace click en Download, reutilizando ``wait_for_download``.

Los DAGs y scripts externos deben invocar ``extraer_dynamic_checklist()`` para
mantener un único punto de entrada, el cual carga automáticamente desde config YAML.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, Optional

# Configurar path para imports cuando se ejecuta directamente
current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[3]))  # /.../proyectos
sys.path.insert(0, str(current_path.parents[4]))  # repo root for other imports

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from clients.auth import AuthManager
from clients.browser import BrowserManager, setup_browser_with_proxy
from clients.filters import FilterManager
from clients.iframes import IframeManager
from clients.date_filter_manager import DateFilterManager
from clients.log_management_manager import LogManagementManager
from common import (
    click_with_retry,
    monitor_export_loader,
    navigate_to_menu_item,
    navigate_to_submenu,
    require,
    wait_for_download,
    wait_for_notification_to_clear,
)
from common.dynamic_checklist_constants import (
    MENU_INDEX_DYNAMIC_CHECKLIST,
    SPLITBUTTON_TIMEOUT,
    LOADER_TIMEOUT,
    DOWNLOAD_TIMEOUT,
    DEFAULT_STATUS_POLL_INTERVAL,
    BUTTON_FILTER,
    BUTTON_EXPORT,
    MENU_DYNAMIC_CHECKLIST,
    SUBMENU_SUB_PM_QUERY,
    LABEL_SUB_PM_QUERY,
    CSS_SPLITBUTTON_TEXT,
    XPATH_SPLITBUTTON_BY_TEXT,
    XPATH_PAGINATION_TOTAL,
    XPATH_LOADING_MASK,
    XPATH_SUBMENU_SUB_PM_QUERY,
)
from core.utils import load_config
from core.helpers import default_download_path

logger = logging.getLogger(__name__)

# Alternativas de texto para botones (español/inglés)
BUTTON_ALTERNATIVES = {"Filtrar": "Filter", "Filter": "Filtrar"}

# Constantes de delays (en segundos) para estabilidad de UI
DELAY_SHORT = 0.1  # Para eventos de UI inmediatos (clicks, inputs)
DELAY_MEDIUM = 0.2  # Para cambios de estado de elementos
DELAY_NORMAL = 0.3  # Para transiciones de UI estándar
DELAY_LONG = 1.0  # Para transiciones completas (cambios de página, modales)
DELAY_VERY_LONG = 2.0  # Para operaciones que requieren procesamiento del backend


@dataclass
class DynamicChecklistConfig:
    """Configuración para el workflow Dynamic Checklist extraída directamente desde config YAML."""
    username: str
    password: str
    download_path: str
    date_mode: int = 2
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    last_n_days: Optional[int] = None
    dynamic_checklist_output_filename: Optional[str] = None
    max_iframe_attempts: int = 60
    max_status_attempts: int = 60
    export_overwrite_files: bool = True
    proxy: Optional[str] = None
    headless: bool = False

    @classmethod
    def from_yaml_config(cls, env: Optional[str] = None, overrides: Optional[Dict[str, Any]] = None) -> "DynamicChecklistConfig":
        """
        Carga configuración desde config YAML (teleows + dynamic_checklist).

        Args:
            env: Entorno (dev, prod, staging)
            overrides: Diccionario con overrides (desde Airflow Connection/Variables)

        Returns:
            DynamicChecklistConfig con la configuración combinada
        """
        config = load_config(env=env)
        teleows_config = config.get("teleows", {})
        dc_config = config.get("dynamic_checklist", {})

        # Combinar configuraciones: teleows (general) + dynamic_checklist (específico) + overrides
        combined = {**teleows_config, **dc_config}
        if overrides:
            combined.update(overrides)

        # Asegurar que download_path existe
        download_path = combined.get("download_path") or combined.get("local_dir") or default_download_path()
        download_path_obj = Path(download_path).expanduser().resolve()
        download_path_obj.mkdir(parents=True, exist_ok=True)

        return cls(
            username=combined.get("username") or "",
            password=combined.get("password") or "",
            download_path=str(download_path_obj),
            date_mode=int(combined.get("date_mode", 2)),
            date_from=combined.get("date_from"),
            date_to=combined.get("date_to"),
            last_n_days=int(combined["last_n_days"]) if combined.get("last_n_days") else None,
            dynamic_checklist_output_filename=combined.get("specific_filename") or combined.get("dynamic_checklist_output_filename"),
            max_iframe_attempts=int(combined.get("max_iframe_attempts", 60)),
            max_status_attempts=int(combined.get("max_status_attempts", 60)),
            export_overwrite_files=bool(combined.get("export_overwrite_files", True)),
            proxy=combined.get("proxy"),
            headless=bool(combined.get("headless", False)),
        )


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
        config: DynamicChecklistConfig,
        headless: Optional[bool] = None,
        chrome_extra_args: Optional[Iterable[str]] = None,
        status_timeout: Optional[int] = None,
        status_poll_interval: Optional[int] = None,
        output_filename: Optional[str] = None,
    ) -> None:
        self.config = config
        self.status_timeout = status_timeout or config.max_status_attempts * DEFAULT_STATUS_POLL_INTERVAL
        self.status_poll_interval = status_poll_interval or DEFAULT_STATUS_POLL_INTERVAL
        self.desired_filename = (output_filename or config.dynamic_checklist_output_filename or "").strip() or None
        self.download_dir = Path(config.download_path).resolve()
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._overwrite_files = config.export_overwrite_files
        self.run_start = None  # Se inicializa en run()

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
            auth_manager.login(self.config.username, self.config.password),
            "No se pudo realizar el login.",
        )
        require(
            self.iframe_manager.find_main_iframe(max_attempts=self.config.max_iframe_attempts),
            "No se encontró el iframe principal",
        )
        self.iframe_manager.switch_to_default_content()
        self._navigate_to_dynamic_checklist()

    def _apply_filters(self) -> None:
        """Aplica los filtros necesarios para la consulta."""
        self._prepare_filters()
        
        # Log de debugging para verificar configuración de fechas
        logger.debug("Configuración de fechas: date_mode=%s, last_n_days=%s, date_from=%s, date_to=%s",
                   self.config.date_mode, self.config.last_n_days,
                   self.config.date_from, self.config.date_to)

        self.date_filter_manager.apply_date_filters(self.config)
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
        logger.info("Script completado exitosamente")
        return downloaded

    def close(self) -> None:
        logger.debug("Cerrando navegador...")
        self.browser_manager.close_driver()

    # ========================================================================
    # Configuración inicial
    # ========================================================================

    def _setup_browser(
        self, headless: Optional[bool], chrome_extra_args: Optional[Iterable[str]]
    ) -> BrowserManager:
        """Configura y crea la instancia de BrowserManager con manejo de proxy."""
        # Usar helper compartido para configuración de browser con proxy
        return setup_browser_with_proxy(
            download_path=str(self.download_dir),
            proxy=self.config.proxy,
            headless=self.config.headless if headless is None else headless,
            chrome_extra_args=chrome_extra_args,
        )

    # ========================================================================
    # Navegación y configuración inicial
    # ========================================================================

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
        logger.debug("Esperando a que cargue la sección Sub PM Query...")
        self._switch_to_last_iframe(LABEL_SUB_PM_QUERY)
        self.filter_manager.wait_for_filters_ready()
        logger.debug("Sección Sub PM Query cargada correctamente (iframe con filtros activo)")
        logger.debug("Aplicando filtros...")
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
            logger.debug("Intentando con texto alternativo: '%s'", alt_label)
            try:
                return extended_wait.until(
                    EC.element_to_be_clickable((By.XPATH, XPATH_SPLITBUTTON_BY_TEXT.format(text=alt_label)))
                )
            except TimeoutException:
                pass
        
        # Intento 3: Búsqueda alternativa - buscar todos los splitbuttons y encontrar uno que coincida
        logger.warning("Intentando buscar cualquier splitbutton disponible...")
        buttons = self.driver.find_elements(By.CSS_SELECTOR, CSS_SPLITBUTTON_TEXT)
        logger.debug("Buscando botones splitbutton...")
        require(len(buttons) > 0, "No se encontraron botones splitbutton disponibles")
        
        logger.debug("Encontrados %s botones splitbutton", len(buttons))
        search_labels = [label]
        if label in BUTTON_ALTERNATIVES:
            search_labels.append(BUTTON_ALTERNATIVES[label])
        
        for btn in buttons:
            btn_text = btn.text.strip()
            logger.debug("Botón encontrado: '%s'", btn_text)
            if any(search_label.lower() in btn_text.lower() for search_label in search_labels):
                return btn
        
        logger.debug("No se pudo encontrar el botón '%s' o sus variantes después de todos los intentos", label)
        require(False, f"No se pudo encontrar el botón '{label}' o sus variantes")

    def _click_splitbutton(self, label: str, pause: int = 2) -> None:
        """Hace click en un splitbutton por su texto visible, con fallback automático."""
        button = self._find_splitbutton_with_fallback(label)
        click_with_retry(button, self.driver)
        logger.debug("Botón '%s' presionado", label)
        if pause:
            sleep(DELAY_VERY_LONG if pause >= 2 else DELAY_LONG if pause >= 1 else pause)

    def _wait_for_list(self) -> None:
        """Confirma que la tabla principal esté disponible tras aplicar filtros."""
        logger.debug("Esperando a que cargue la lista...")
        # Aprovechamos el texto "Total" de la paginación para validar que la tabla ya está renderizada.
        total_element = self.wait.until(
            EC.presence_of_element_located((By.XPATH, XPATH_PAGINATION_TOTAL))
        )
        logger.debug("Lista cargada: %s", total_element.text)

    # ========================================================================
    # Exportación y monitoreo
    # ========================================================================

    def _wait_for_loader(self) -> None:
        """Detecta el loader posterior a la exportación (asegura que se disparó)."""
        logger.debug("Esperando confirmación de exportación...")
        try:
            self.wait.until(EC.presence_of_element_located((By.XPATH, XPATH_LOADING_MASK)))
            wait_for_notification_to_clear(self.driver, timeout=LOADER_TIMEOUT)
        except TimeoutException:
            logger.warning("No se detectó loader tras la exportación (posible fallo silencioso)")



# ========================================================================
# Funciones públicas de entrada
# ========================================================================

def run_dynamic_checklist(
    config: DynamicChecklistConfig,
    *,
    headless: Optional[bool] = None,
    chrome_extra_args: Optional[Iterable[str]] = None,
    status_timeout: Optional[int] = None,
    status_poll_interval: Optional[int] = None,
    output_filename: Optional[str] = None,
) -> Path:
    """
    Ejecuta el flujo completo de Dynamic Checklist y devuelve la ruta del archivo descargado.

    Args:
        config: Instancia de ``DynamicChecklistConfig`` con la configuración.
        headless / chrome_extra_args: Overrides opcionales para el navegador.
        status_timeout / status_poll_interval: Timeouts para monitoreo.
        output_filename: Nombre del archivo de salida.

    Returns:
        ``Path`` absoluto del archivo descargado.
    """
    workflow = DynamicChecklistWorkflow(
        config=config,
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
    config: Optional[DynamicChecklistConfig] = None,
    *,
    env: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
    headless: Optional[bool] = None,
    chrome_extra_args: Optional[Iterable[str]] = None,
    status_timeout: Optional[int] = None,
    status_poll_interval: Optional[int] = None,
    output_filename: Optional[str] = None,
) -> str:
    """
    Ejecuta el workflow Dynamic Checklist y devuelve la ruta del archivo exportado.

    Args:
        config: Configuración Dynamic Checklist (si no se proporciona, se carga desde YAML)
        env: Entorno (dev, prod, staging) - usado si config es None
        overrides: Diccionario con overrides desde Airflow
        headless: Modo headless del navegador
        chrome_extra_args: Argumentos adicionales para Chrome
        status_timeout: Timeout para monitoreo de status
        status_poll_interval: Intervalo de polling
        output_filename: Nombre del archivo de salida

    Returns:
        Ruta del archivo descargado como string
    """
    if config is None:
        config = DynamicChecklistConfig.from_yaml_config(env=env, overrides=overrides)

    logger.info(
        "Iniciando extracción Dynamic Checklist (env=%s, overrides=%s)",
        env or "default",
        sorted(overrides.keys()) if overrides else "none",
    )
    path = run_dynamic_checklist(
        config,
        headless=headless,
        chrome_extra_args=chrome_extra_args,
        status_timeout=status_timeout,
        status_poll_interval=status_poll_interval,
        output_filename=output_filename,
    )
    logger.info("Extracción Dynamic Checklist finalizada. Archivo: %s", path)
    return str(path)


# Ejecución local (desarrollo/testing)
# Para producción, usar los DAGs de Airflow en dags/DAG_dynamic_checklist.py
# El entorno se determina automáticamente desde ENV_MODE o usa "dev" por defecto
if __name__ == "__main__":
    extraer_dynamic_checklist(
        headless=False,
        overrides={"proxy": None},
    )
