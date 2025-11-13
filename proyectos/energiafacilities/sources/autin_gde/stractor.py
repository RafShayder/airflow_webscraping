"""
Workflow GDE: automatiza la descarga del reporte Console GDE Export.

=== Flujo general ===
1) Configuración y navegador
   - ``GDEConfig`` carga credenciales, proxy, filtros, rutas desde config YAML.
   - ``GDEWorkflow`` encapsula el flujo completo (enfoque OOP, consistente con Checklist).
   - ``BrowserManager`` (clients.browser) crea el driver de Selenium.
2) Login y contexto
   - ``AuthManager`` realiza la autenticación.
   - ``IframeManager`` localiza el iframe principal; ``FilterManager`` coordina
     la apertura del panel de filtros.
3) Preparación de filtros (helpers de este archivo):
   - ``_click_clear_filters`` garantiza partir de un estado limpio.
   - ``_apply_task_type_filters`` aplica selección de tipos (CM/OPM).
   - ``DateFilterManager`` aplica filtros de fecha (unificado con Dynamic Checklist).
   - ``_apply_filters`` usa el botón caret "Filtrar", espera el refresco y selecciona una fila.
4) Exportación y monitoreo
   - ``_trigger_export`` lanza la exportación y retorna un timestamp de control.
   - ``_navigate_to_export_status`` y ``_monitor_status`` esperan a que el
     backend termine el procesamiento (tabla de Export Status).
5) Descarga final
   - ``_download_export`` detecta el archivo dentro del directorio de descargas
     utilizando ``wait_for_download`` para renombrarlo o resolver conflictos.

Los DAGs y scripts externos deben invocar ``extraer_gde()`` para mantener un
único punto de entrada, el cual carga automáticamente desde config YAML.

Para uso avanzado, se puede usar directamente ``GDEWorkflow`` (similar a ``DynamicChecklistWorkflow``).

"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, List, Optional

current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[3]))  # /.../proyectos
sys.path.insert(0, str(current_path.parents[4]))  # repo root for other imports

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
from selenium.webdriver.common.keys import Keys

from energiafacilities.clients.auth import AuthManager
from energiafacilities.clients.browser import BrowserManager, setup_browser_with_proxy
from energiafacilities.clients.date_filter_manager import DateFilterManager
from energiafacilities.clients.filters import FilterManager
from energiafacilities.clients.iframes import IframeManager
from energiafacilities.common import require, wait_for_download
from energiafacilities.core.utils import load_config, default_download_path

logger = logging.getLogger(__name__)

# Constantes de selectores XPATH
FILTER_BUTTON_XPATH = "//span[.//i[contains(@class,'el-icon-caret-right')] and contains(normalize-space(),'Filtrar')]"
CREATETIME_FROM_XPATH = "//*[@id='createtimeRow']/div[2]/div[2]/div/div[2]/div/div[1]"
CREATETIME_TO_XPATH = "//*[@id='createtimeRow']/div[2]/div[2]/div/div[2]/div/div[2]"

# Constantes de delays (en segundos) para estabilidad de UI
DELAY_SHORT = 0.1  # Para eventos de UI inmediatos (clicks, inputs)
DELAY_MEDIUM = 0.2  # Para cambios de estado de elementos
DELAY_NORMAL = 0.3  # Para transiciones de UI estándar
DELAY_LONG = 1.0  # Para transiciones completas (cambios de página, modales)
DELAY_VERY_LONG = 2.0  # Para operaciones que requieren procesamiento del backend


@dataclass
class GDEConfig:
    """Configuración para el workflow GDE extraída directamente desde config YAML."""
    username: str
    password: str
    download_path: str
    options_to_select: List[str]
    date_mode: int = 2
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    last_n_days: Optional[int] = None
    gde_output_filename: Optional[str] = None
    max_iframe_attempts: int = 60
    max_status_attempts: int = 60
    export_overwrite_files: bool = True
    proxy: Optional[str] = None
    headless: bool = False

    @classmethod
    def from_yaml_config(cls, env: Optional[str] = None, overrides: Optional[Dict[str, Any]] = None) -> "GDEConfig":
        """
        Carga configuración desde config YAML (teleows + gde).

        Args:
            env: Entorno (dev, prod, staging)
            overrides: Diccionario con overrides (desde Airflow Connection/Variables)

        Returns:
            GDEConfig con la configuración combinada
        """
        config = load_config(env=env)
        teleows_config = config.get("teleows", {})
        gde_config = config.get("gde", {})

        # Combinar configuraciones: teleows (general) + gde (específico) + overrides
        combined = {**teleows_config, **gde_config}
        if overrides:
            combined.update(overrides)

        # Parsear options_to_select si viene como string
        options = combined.get("options_to_select", "CM,OPM")
        if isinstance(options, str):
            options = [opt.strip() for opt in options.split(",")]

        # Asegurar que download_path existe
        download_path = combined.get("download_path") or combined.get("local_dir") or default_download_path()
        download_path_obj = Path(download_path).expanduser().resolve()
        download_path_obj.mkdir(parents=True, exist_ok=True)

        return cls(
            username=combined.get("username") or "",
            password=combined.get("password") or "",
            download_path=str(download_path_obj),
            options_to_select=options,
            date_mode=int(combined.get("date_mode", 2)),
            date_from=combined.get("date_from"),
            date_to=combined.get("date_to"),
            last_n_days=int(combined["last_n_days"]) if combined.get("last_n_days") else None,
            gde_output_filename=combined.get("specific_filename") or combined.get("gde_output_filename"),
            max_iframe_attempts=int(combined.get("max_iframe_attempts", 60)),
            max_status_attempts=int(combined.get("max_status_attempts", 60)),
            export_overwrite_files=bool(combined.get("export_overwrite_files", True)),
            proxy=combined.get("proxy"),
            headless=bool(combined.get("headless", False)),
        )


# ========================================================================
# Clase principal de workflow
# ========================================================================

class GDEWorkflow:
    """
    Implementa el flujo completo de GDE.
    
    Componentes clave utilizados:
        - ``BrowserManager``: instancia del driver de Selenium con rutas de descarga.
        - ``AuthManager``: login al portal Integratel.
        - ``IframeManager``: cambio entre iframes para acceder a filtros y Export Status.
        - ``FilterManager``: apertura/esperas del panel de filtros.
        - ``DateFilterManager``: aplicación de filtros de fecha.
        - Helpers locales: encapsulan interacciones específicas de la UI.
    """
    
    def __init__(
        self,
        config: GDEConfig,
        *,
        headless: Optional[bool] = None,
        chrome_extra_args: Optional[Iterable[str]] = None,
        status_timeout: Optional[int] = None,
        status_poll_interval: Optional[int] = None,
        output_filename: Optional[str] = None,
    ) -> None:
        self.config = config
        self.status_timeout = status_timeout or config.max_status_attempts * 30
        self.status_poll_interval = status_poll_interval or 8
        self.desired_filename = (output_filename or config.gde_output_filename or "").strip() or None
        self.download_dir = Path(config.download_path).resolve()
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._overwrite_files = config.export_overwrite_files
        self.export_started = None  # Se inicializa en _export_and_download()
        
        # Inicializar managers
        self.browser_manager = self._setup_browser(headless, chrome_extra_args)
        self.driver, self.wait = self.browser_manager.create_driver()
        self.iframe_manager = IframeManager(self.driver)
        self.filter_manager = FilterManager(self.driver, self.wait)
        self.date_filter_manager = DateFilterManager(self.driver, self.wait)
    
    def _setup_browser(
        self, headless: Optional[bool], chrome_extra_args: Optional[Iterable[str]]
    ) -> BrowserManager:
        """Configura y crea la instancia de BrowserManager con manejo de proxy."""
        return setup_browser_with_proxy(
            download_path=str(self.download_dir),
            proxy=self.config.proxy,
            headless=self.config.headless if headless is None else headless,
            chrome_extra_args=chrome_extra_args,
        )
    
    def run(self) -> Path:
        """Ejecuta el flujo completo de GDE y devuelve la ruta del archivo descargado."""
        self._authenticate_and_navigate()
        self._apply_filters()
        return self._export_and_download()
    
    def _authenticate_and_navigate(self) -> None:
        """Autentica y navega al módulo GDE."""
        auth_manager = AuthManager(self.driver)
        require(
            auth_manager.login(self.config.username, self.config.password),
            "No se pudo realizar el login",
        )
        require(
            self.iframe_manager.find_main_iframe(max_attempts=self.config.max_iframe_attempts),
            "No se encontró el iframe principal",
        )
        
        self.filter_manager.wait_for_filters_ready()
        self.filter_manager.open_filter_panel(method="complex")
    
    def _apply_filters(self) -> None:
        """Aplica los filtros necesarios para la consulta."""
        _click_clear_filters(self.driver, self.wait)
        _apply_task_type_filters(self.driver, self.wait, self.config.options_to_select)
        
        # Aplicar filtros de fecha según date_mode
        if self.config.date_mode == 1:
            _apply_gde_manual_dates(self.driver, self.wait, self.config)
        else:
            self.date_filter_manager.apply_date_filters(self.config)
        
        _apply_filters(self.driver)
    
    def _export_and_download(self) -> Path:
        """Ejecuta la exportación y maneja la descarga del archivo."""
        self.export_started = _trigger_export(self.driver)
        
        _navigate_to_export_status(self.iframe_manager)
        _monitor_status(
            self.driver,
            self.status_timeout,
            self.status_poll_interval,
        )
        
        downloaded = _download_export(
            self.driver,
            self.download_dir,
            self.export_started,
            overwrite_files=self._overwrite_files,
            output_filename=self.desired_filename,
        )
        
        logger.info("Flujo GDE completado")
        return downloaded
    
    def close(self) -> None:
        """Cierra el navegador."""
        logger.debug("Cerrando navegador...")
        self.browser_manager.close_driver()


# ========================================================================
# Helpers de UI (clicks robustos, navegación de iframes)
# ========================================================================

def _robust_click(driver, elem):
    """Intenta click con varias estrategias (igual que en test.py)."""
    try:
        elem.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elem)
            sleep(DELAY_MEDIUM)
            elem.click()
            return True
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", elem)
                return True
            except Exception:
                logger.debug("No se pudo hacer click en el elemento después de intentar todas las estrategias")
                return False


def _switch_to_frame_with(driver, css_or_xpath: str) -> bool:
    """Replica la utilidad usada en test.py para ubicar el iframe correcto."""
    driver.switch_to.default_content()
    locator = (By.XPATH, css_or_xpath) if css_or_xpath.startswith("//") else (By.CSS_SELECTOR, css_or_xpath)

    try:
        driver.find_element(*locator)
        return True
    except Exception:
        pass

    frames = driver.find_elements(By.TAG_NAME, "iframe")
    for frame in frames:
        driver.switch_to.default_content()
        driver.switch_to.frame(frame)
        try:
            driver.find_element(*locator)
            return True
        except Exception:
            continue

    driver.switch_to.default_content()
    logger.debug("No se pudo encontrar el iframe con selector: %s", css_or_xpath)
    return False


# ========================================================================
# Gestión de filtros
# ========================================================================

def _click_clear_filters(driver, wait) -> None:
    """Limpia filtros anteriores para evitar arrastrar configuraciones previas.

    La UI de Integratel conserva la última selección, así que se fuerza un reset
    antes de aplicar la nueva combinación (CM/OPM + fechas).
    """
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="allTask_tab"]/form/div[2]/div/div/div[2]/button[2]')
        )
    ).click()
    logger.debug("Filtros anteriores limpiados")
    sleep(DELAY_LONG)


def _apply_task_type_filters(driver, wait, options: Iterable[str]) -> None:
    """Marca las opciones de Task Type indicadas en TeleowsSettings.options_to_select.

    ``options`` viene de settings (por defecto ["CM", "OPM"]). El helper abre
    el combo y clickea uno por uno manejando el retardo en el DOM.
    """
    logger.debug("Asignando opciones en Task Type...")
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#all_taskType .el-select__caret"))).click()
    sleep(DELAY_LONG)

    for option in options:
        xpath = f"//li[contains(@class, 'el-select-dropdown__item') and @title='{option}']"
        wait.until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
        logger.debug("Opción seleccionada: %s", option)
        sleep(DELAY_NORMAL)


def _apply_filters(driver) -> None:
    """Aplica los filtros usando el botón Filtrar con ícono caret y espera la carga."""
    wait = WebDriverWait(driver, 15)
    logger.debug("Aplicando filtros (botón caret + espera de tabla)...")
    _click_filter_button(driver, wait)
    _wait_for_filters_to_apply(driver, wait)
    if not _select_first_grid_row(driver, wait):
        logger.warning("No se pudo seleccionar una fila después de aplicar filtros")


def _click_filter_button(driver, wait: WebDriverWait) -> None:
    """Hace click en el span que contiene el ícono caret + texto 'Filtrar'."""
    try:
        filter_button = wait.until(EC.element_to_be_clickable((By.XPATH, FILTER_BUTTON_XPATH)))
    except TimeoutException:
        filter_button = driver.find_element(By.XPATH, FILTER_BUTTON_XPATH)

    try:
        filter_button.click()
    except Exception:
        driver.execute_script("arguments[0].click();", filter_button)


def _wait_for_filters_to_apply(driver, wait: WebDriverWait) -> None:
    """Espera a que el panel de filtros se cierre y la tabla se refresque."""
    try:
        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".el-drawer__wrapper")))
    except TimeoutException:
        logger.warning("El panel de filtros no se cerró a tiempo")

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#allTask_tab .el-loading-mask"))
        )
    except TimeoutException:
        logger.debug("No apareció loading mask al aplicar filtros")

    try:
        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "#allTask_tab .el-loading-mask")))
    except TimeoutException:
        logger.warning("Loading mask permaneció visible más de lo esperado")

    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#allTask_tab .el-table__body tr")))
    sleep(DELAY_NORMAL)


# ========================================================================
# Gestión de fechas manuales
# ========================================================================

def _resolve_manual_date_range(config: GDEConfig) -> tuple[Optional[str], Optional[str]]:
    """Resuelve el rango de fechas desde la configuración."""
    if config.last_n_days:
        date_to = datetime.now()
        date_from = date_to - timedelta(days=config.last_n_days)
        return date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d")
    return config.date_from, config.date_to


def _click_y_setear_fecha(driver, wait: WebDriverWait, container_xpath: str, fecha: str) -> None:
    """Abre el selector de fecha y establece el valor (extraído de función anidada).
    
    Replica exactamente la lógica de test.py para establecer fechas en el selector.
    
    Args:
        driver: Instancia de WebDriver
        wait: WebDriverWait para esperas explícitas
        container_xpath: XPATH del contenedor (DESDE o HASTA)
        fecha: Fecha en formato string (YYYY-MM-DD)
        
    Raises:
        RuntimeError: Si no se encuentra el input o no se puede escribir la fecha
    """
    # 1) Intenta abrir el modo "intervalo personalizado" con el botón +
    try:
        plus = driver.find_element(By.CSS_SELECTOR, ".ows_datetime_interval_customer_text.el-icon-circle-plus")
        if plus.is_displayed():
            _robust_click(driver, plus)
            sleep(DELAY_MEDIUM)
    except Exception:
        pass  # si no existe, seguimos

    # 2) Click en el contenedor (DESDE o HASTA)
    cont = wait.until(EC.element_to_be_clickable((By.XPATH, container_xpath)))
    _robust_click(driver, cont)
    sleep(DELAY_MEDIUM)

    # 3) Esperar a que el picker panel aparezca (específico para headless/Airflow)
    # En headless, el popover puede tardar más en renderizarse o estar fuera del viewport
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".el-picker-panel, .el-date-picker"))
        )
        logger.debug("Picker panel detectado")
        sleep(DELAY_SHORT)  # Dar tiempo adicional para que se renderice completamente
    except TimeoutException:
        logger.debug("Picker panel no apareció explícitamente, continuando de todas formas")

    # 4) Click en el input "Seleccionar fecha" y setear valor
    # En headless/Airflow: NO depender de is_displayed() - el elemento puede existir pero no estar "visible"
    target = None
    
    # Estrategia 1: Buscar todos los inputs y usar JavaScript para verificar visibilidad real
    inputs = driver.find_elements(By.CSS_SELECTOR, 'input.el-input__inner[placeholder="Seleccionar fecha"]')
    for el in inputs:
        try:
            # Usar JavaScript para verificar si el elemento está realmente renderizado
            # (más confiable que is_displayed() en headless)
            is_actually_visible = driver.execute_script("""
                var el = arguments[0];
                var rect = el.getBoundingClientRect();
                var style = window.getComputedStyle(el);
                return rect.width > 0 && rect.height > 0 && 
                       style.display !== 'none' && 
                       style.visibility !== 'hidden' &&
                       style.opacity !== '0';
            """, el)
            
            if is_actually_visible:
                target = el
                logger.debug("Input encontrado usando verificación JavaScript")
                break
        except Exception:
            # Si falla la verificación JS, usar el elemento de todas formas
            if target is None:
                target = el

    # Estrategia 2: Si no encontramos ninguno "visible", tomar el último (suele ser el del popover)
    if target is None and inputs:
        target = inputs[-1]
        logger.debug("Usando último input encontrado (fallback para headless)")

    # Estrategia 3: Buscar dentro del propio contenedor
    if target is None:
        try:
            target = cont.find_element(By.CSS_SELECTOR, 'input.el-input__inner[placeholder="Seleccionar fecha"]')
            logger.debug("Input encontrado dentro del contenedor")
        except Exception:
            pass

    logger.debug("Buscando input 'Seleccionar fecha'...")
    require(target is not None, "No se encontró el input 'Seleccionar fecha' después de abrir el selector.")

    # Escribir la fecha de forma robusta
    try:
        target.click()
        sleep(DELAY_SHORT)
        target.send_keys(Keys.CONTROL, 'a')
        target.send_keys(Keys.DELETE)
        target.send_keys(fecha)
        # Disparar eventos para que el framework (Element UI) detecte el cambio
        driver.execute_script(
            "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
            "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
            target
        )
        target.send_keys(Keys.ENTER)  # confirma/cierra el popover
        sleep(DELAY_MEDIUM)
    except Exception as e:
        logger.debug("No se pudo escribir la fecha '%s': %s", fecha, e)
        raise RuntimeError(f"No se pudo escribir la fecha '{fecha}': {e}")


def _confirmar_selector_fecha(driver) -> None:
    """Confirma el selector de fecha enviando ENTER al elemento activo.
    
    Función helper extraída para mejorar legibilidad y testabilidad.
    
    Args:
        driver: Instancia de WebDriver
    """
    try:
        active_input = driver.switch_to.active_element
        active_input.send_keys(Keys.ENTER)
        sleep(DELAY_MEDIUM)
    except Exception:
        pass


def _apply_gde_manual_dates(
    driver,
    wait: WebDriverWait,
    config: GDEConfig,
) -> None:
    """Aplica fechas manuales para Create Time usando la lógica de test.py.
    
    Replica exactamente la lógica de test.py para Create Time (sin verificaciones extras).
    Primero establece la fecha DESDE, luego HASTA.
    
    Args:
        driver: Instancia de WebDriver
        wait: WebDriverWait para esperas explícitas
        config: Configuración GDE con fechas
    """
    # Asegúrate de estar en el iframe de filtros (igual que test.py)
    _switch_to_frame_with(driver, ".ows_filter_title")
    
    date_from, date_to = _resolve_manual_date_range(config)
    logger.debug("Aplicando fechas manuales: DESDE %s → HASTA %s", date_from, date_to)

    # === DESDE ===
    _click_y_setear_fecha(driver, wait, CREATETIME_FROM_XPATH, date_from)
    logger.debug("Fecha DESDE aplicada: %s", date_from)
    sleep(DELAY_NORMAL)

    # === HASTA ===
    _click_y_setear_fecha(driver, wait, CREATETIME_TO_XPATH, date_to)
    logger.debug("Fecha HASTA aplicada: %s", date_to)
    sleep(DELAY_NORMAL)
    _confirmar_selector_fecha(driver)

    logger.debug("Fechas manuales aplicadas: %s → %s", date_from, date_to)


# ========================================================================
# Gestión de advertencias y selección de filas
# ========================================================================

def _handle_filter_warning(driver, wait: WebDriverWait, *, reapply_filter: bool = True) -> bool:
    """Cierra la advertencia 'Select at least one record' y reaplica filtros si se solicita."""
    try:
        prompt = WebDriverWait(driver, 3).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, ".prompt-wrapper.vigour-prompt.prompt-type-alert"))
        )
    except TimeoutException:
        logger.debug("No se encontró advertencia de filtros (timeout)")
        return False

    accept_button = prompt.find_element(
        By.XPATH,
        "//button[contains(@class,'sdm_button_primary') and .//span[normalize-space()='Aceptar']]",
    )
    try:
        accept_button.click()
    except Exception:
        driver.execute_script("arguments[0].click();", accept_button)

    wait.until(
        EC.invisibility_of_element_located((By.CSS_SELECTOR, ".prompt-wrapper.vigour-prompt.prompt-type-alert"))
    )

    if reapply_filter:
        logger.debug("Reaplicando filtros tras advertencia...")
        _click_filter_button(driver, wait)
        _wait_for_filters_to_apply(driver, wait)
    return True


def _select_first_grid_row(driver, wait: WebDriverWait) -> bool:
    """Selecciona la primera fila del grid marcando la casilla de selección."""
    checkbox_selector = (
        "#allTask_tab .el-table__body tr:first-child td.el-table-column--selection .el-checkbox__inner"
    )

    for attempt in range(2):
        try:
            checkbox = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, checkbox_selector)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", checkbox)
            checkbox.click()
            sleep(DELAY_MEDIUM)
            logger.debug("Primera fila seleccionada")
            return True
        except ElementClickInterceptedException:
            logger.debug("Click interceptado al seleccionar fila (intento %s)", attempt + 1)
            if not _handle_filter_warning(driver, wait):
                break
        except TimeoutException:
            logger.debug("No se encontró fila seleccionable (timeout)")
            break
    logger.debug("No se pudo seleccionar la primera fila después de todos los intentos")
    return False


# ========================================================================
# Exportación y monitoreo de estado
# ========================================================================

def _click_export_button(driver) -> None:
    """Intenta hacer click en Exportar, cerrando advertencias si bloquean el botón."""
    wait = WebDriverWait(driver, 12)
    for attempt in range(3):
        try:
            export_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#test > .sdm_splitbutton_text"))
            )
            export_button.click()
            sleep(DELAY_LONG)
            logger.debug("Exportar presionado")
            return
        except ElementClickInterceptedException:
            logger.warning(
                "Exportar bloqueado por advertencia (intento %s), cerrando modal...",
                attempt + 1,
            )
            if not _handle_filter_warning(driver, wait):
                raise
            _select_first_grid_row(driver, wait)
    logger.debug("No se pudo presionar el botón de exportación después de 3 intentos")
    require(False, "No se pudo presionar el botón de exportación")


def _trigger_export(driver) -> float:
    """Hace click en el botón de exportación manejando advertencias previas."""
    logger.debug("Disparando exportación...")
    _click_export_button(driver)
    return time.time()


def _navigate_to_export_status(iframe_manager: IframeManager) -> None:
    """Cierra el popup de resultados y abre el módulo de estado de exportación.

    Después de lanzar la exportación, Integratel muestra un modal de éxito que
    debe cerrarse. Luego se navega en el menú lateral para entrar a Export
    Status, cambiando de iframe con ``IframeManager`` (teleows.clients.iframes).
    """
    iframe_manager.switch_to_default_content()
    logger.debug("Navegando a sección de export status...")
    iframe_manager.driver.find_element(By.CSS_SELECTOR, ".el-icon-close:nth-child(2)").click()
    sleep(DELAY_LONG)

    driver = iframe_manager.driver
    driver.find_element(By.CSS_SELECTOR, ".el-row:nth-child(6) > .side-item-icon").click()
    sleep(DELAY_LONG)
    driver.find_element(By.CSS_SELECTOR, ".level-1").click()
    sleep(DELAY_LONG)
    require(iframe_manager.switch_to_iframe(1), "No se pudo cambiar al iframe de export status")
    sleep(DELAY_LONG)


def _monitor_status(driver, timeout_seconds: int, poll_interval: int) -> None:
    """Revisa el panel de exportación hasta que el job finaliza (éxito o error).

    El panel actualiza cada vez que se pulsa el ícono de refresh. Se revisa el
    texto de la primera fila (estado). Al detectar un estado final distinto de
    Succeed se lanza una excepción para que el DAG lo refleje como fallo.
    """
    logger.debug("Iniciando monitoreo de estado de exportación...")
    end_states = {"Succeed", "Failed", "Aborted", "Waiting", "Concurrent Waiting"}
    deadline = time.time() + timeout_seconds
    attempt = 0

    while time.time() < deadline:
        attempt += 1
        try:
            driver.find_element(By.CSS_SELECTOR, "span.button_icon.btnIcon[style*='refresh']").click()
            logger.debug("Refresh intento %s (restan %.0f s)", attempt, deadline - time.time())
        except Exception as exc:
            logger.warning("No se pudo presionar Refresh: %s", exc, exc_info=True)

        sleep(DELAY_VERY_LONG)
        status = driver.find_element(
            By.XPATH, '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]/td[3]/div/span'
        ).text.strip()
        logger.debug("Estado de exportación: %s", status)

        if status in end_states:
            if status == "Succeed":
                logger.info("Exportación completada exitosamente")
                return
            logger.debug("Proceso de exportación terminó con estado: %s", status)
            require(False, f"Proceso de exportación terminó con estado: {status}")

        if status != "Running":
            logger.warning("Estado desconocido '%s'. Continuando monitoreo...", status)

        sleep(poll_interval)

    logger.debug("Tiempo máximo de espera alcanzado durante el monitoreo de exportación")
    require(False, "Tiempo máximo de espera alcanzado durante el monitoreo de exportación")


# ========================================================================
# Descarga de archivos
# ========================================================================

def _download_export(
    driver,
    download_dir: Path,
    started_at: float,
    *,
    overwrite_files: bool,
    timeout: int = 120,
    output_filename: Optional[str] = None,
) -> Path:
    """Localiza el archivo descargado y lo renombra si se solicitó.

    - ``wait_for_download`` (teleows.common) compara los archivos presentes
      antes y después de la descarga, filtrando *.crdownload.
    - ``output_filename`` puede provenir de settings o del DAG (override).
    """
    logger.debug("Preparando descarga...")
    before = {p for p in download_dir.iterdir() if p.is_file()}
    driver.find_element(
        By.XPATH, '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]/td[11]/div/div[3]'
    ).click()
    logger.debug("Botón de descarga presionado")

    return wait_for_download(
        download_dir,
        since=started_at,
        overwrite=overwrite_files,
        timeout=timeout,
        desired_name=output_filename,
        logger=logger,
        initial_snapshot=tuple(before),
    )


# ========================================================================
# Funciones principales de ejecución
# ========================================================================

def run_gde(
    config: GDEConfig,
    *,
    headless: Optional[bool] = None,
    chrome_extra_args: Optional[Iterable[str]] = None,
    status_timeout: Optional[int] = None,
    status_poll_interval: Optional[int] = None,
    output_filename: Optional[str] = None,
) -> Path:
    """
    Ejecuta el flujo completo de exportación para GDE y devuelve la ruta del archivo descargado.

    Parámetros:
        config:
            Instancia de ``GDEConfig`` con credenciales, filtros, rutas y flags.
            Se crea usando ``GDEConfig.from_yaml_config(env, overrides)``.
        headless / chrome_extra_args:
            Overrides opcionales para el navegador (útiles durante pruebas).
        status_timeout / status_poll_interval:
            Ajustan el tiempo máximo y la frecuencia de refresco al monitorear Export Status.
        output_filename:
            Permite forzar el nombre del archivo final (si no se usa, cae en config.gde_output_filename).

    Devuelve:
        ``Path`` absoluto del archivo final descargado (renombrado si corresponde).
    """
    workflow = GDEWorkflow(
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


def extraer_gde(
    config: Optional[GDEConfig] = None,
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
    Ejecuta el workflow Selenium de GDE y devuelve la ruta del archivo generado.

    Args:
        config: Configuración GDE (si no se proporciona, se carga desde YAML)
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
        resolved_env = env or os.getenv("ENV_MODE")
        config = GDEConfig.from_yaml_config(env=resolved_env, overrides=overrides)
    else:
        resolved_env = env

    logger.info(
        "Iniciando extracción GDE (env=%s, overrides=%s)",
        resolved_env or "default",
        sorted(overrides.keys()) if overrides else "none",
    )
    path = run_gde(
        config,
        headless=headless,
        chrome_extra_args=chrome_extra_args,
        status_timeout=status_timeout,
        status_poll_interval=status_poll_interval,
        output_filename=output_filename,
    )
    logger.info("Extracción GDE finalizada. Archivo: %s", path)
    return str(path)


# Ejecución local (desarrollo/testing)
# Para producción, usar los DAGs de Airflow en dags/DAG_gde.py
if __name__ == "__main__":
    extraer_gde(
        headless=False,
        overrides={"proxy": None},
    )
