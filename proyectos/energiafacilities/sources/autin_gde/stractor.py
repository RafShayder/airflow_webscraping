"""
Workflow GDE: automatiza la descarga del reporte Console GDE Export.

=== Flujo general ===
1) Configuración y navegador
   - ``TeleowsSettings`` aporta credenciales, proxy, filtros, rutas de descarga.
   - ``BrowserManager`` (teleows.clients.browser) crea el driver de Selenium.
2) Login y contexto
   - ``AuthManager`` realiza la autenticación.
   - ``IframeManager`` localiza el iframe principal; ``FilterManager`` coordina
     la apertura del panel de filtros. Ambos viven en teleows.clients.
3) Preparación de filtros (helpers de este archivo):
   - ``_click_clear_filters`` garantiza partir de un estado limpio.
   - ``_apply_task_type_filters`` y ``_apply_date_filters`` aplican la selección
     de tipos y fechas definidos en settings.
   - ``_apply_filters`` realiza el hover/click necesario para confirmar filtros.
4) Exportación y monitoreo
   - ``_trigger_export`` lanza la exportación y retorna un timestamp de control.
   - ``_navigate_to_export_status`` y ``_monitor_status`` esperan a que el
     backend termine el procesamiento (tabla de Export Status).
5) Descarga final
   - ``_download_export`` detecta el archivo dentro del directorio de descargas
     utilizando ``wait_for_download`` (common) para renombrarlo o resolver
     conflictos según la configuración.

Este módulo reemplaza al antiguo ``teleows.GDE`` y centraliza la lógica de
scraping. Los DAGs, scripts y workflows externos deben invocar ``run_gde`` o,
si lo prefieren, ``extraer_gde`` para mantener un único punto de entrada.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, Optional

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from clients.auth import AuthManager
from clients.browser import BrowserManager
from clients.filters import FilterManager
from clients.iframes import  IframeManager
from common.selenium_utils import require, wait_for_download
from config.teleows_config import TeleowsSettings
from core.utils import load_settings

logger = logging.getLogger(__name__)


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
    logger.info("✓ Filtros anteriores limpiados")
    sleep(1)


def _apply_task_type_filters(driver, wait, options: Iterable[str]) -> None:
    """Marca las opciones de Task Type indicadas en TeleowsSettings.options_to_select.

    ``options`` viene de settings (por defecto ["CM", "OPM"]). El helper abre
    el combo y clickea uno por uno manejando el retardo en el DOM.
    """
    logger.info("📋 Asignando opciones en Task Type...")
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#all_taskType .el-select__caret"))).click()
    sleep(1)

    for option in options:
        xpath = f"//li[contains(@class, 'el-select-dropdown__item') and @title='{option}']"
        wait.until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
        logger.info("✓ %s", option)
        sleep(0.3)


def _apply_date_filters(driver, settings: TeleowsSettings) -> None:
    """Configura filtros de fecha (manual o rango rápido según settings.date_mode).

    - date_mode=1 → se inyectan valores manuales vía JavaScript (los inputs están
      hechos con Element UI Vue y requieren actualizar el componente Vue padre).
      Si settings.last_n_days está configurado, calcula dinámicamente las fechas.
    - date_mode=2 → selecciona el radio "Último mes".
    """
    if settings.date_mode == 1:
        # Calcular fechas dinámicamente si last_n_days está configurado
        if settings.last_n_days is not None:
            from datetime import datetime, timedelta
            date_to = datetime.now()
            date_from = date_to - timedelta(days=settings.last_n_days)
            date_from_str = date_from.strftime('%Y-%m-%d')
            date_to_str = date_to.strftime('%Y-%m-%d')
            logger.info("Aplicando últimos %s días: %s → %s", settings.last_n_days, date_from_str, date_to_str)
        else:
            date_from_str = settings.date_from
            date_to_str = settings.date_to
            logger.info("Aplicando fechas manuales: %s → %s", date_from_str, date_to_str)

        wait = WebDriverWait(driver, 10)
        # Esperar a que createtimeRow esté disponible
        wait.until(EC.presence_of_element_located((By.ID, "createtimeRow")))
        createtime_row = driver.find_element(By.ID, "createtimeRow")
        
        # Buscar y hacer clic en el botón "+" para abrir los campos de fecha
        try:
            plus_button = createtime_row.find_element(By.CSS_SELECTOR, "button")
            driver.execute_script("arguments[0].click();", plus_button)
            sleep(2.0)
            logger.info("Campos de fecha personalizados abiertos")
        except Exception:
            logger.debug("Botón '+' no encontrado, campos pueden estar ya abiertos")
            sleep(1.0)
        
        # Buscar los inputs de fecha
        inputs = createtime_row.find_elements(By.CSS_SELECTOR, "input[type='text']")
        if len(inputs) < 2:
            sleep(2.0)
            inputs = createtime_row.find_elements(By.CSS_SELECTOR, "input[type='text']")
        
        if len(inputs) < 2:
            raise RuntimeError(f"No se encontraron suficientes inputs en createtimeRow (encontrados: {len(inputs)})")
        
        input_from_elem = inputs[0]
        input_to_elem = inputs[1]
        
        # Script simplificado para aplicar fecha a componente Vue
        def apply_date(input_element, date_value, date_display):
            """Aplica una fecha al componente Vue y al input."""
            script = """
                const input = arguments[0];
                const dateValue = arguments[1];
                const dateDisplay = arguments[2];
                
                if (!input) return false;
                
                // Buscar componente Vue padre
                let vueComponent = null;
                let parent = input.parentElement;
                while (parent && !vueComponent) {
                    if (parent.__vue__) {
                        vueComponent = parent.__vue__;
                        break;
                    }
                    parent = parent.parentElement;
                }
                
                // Actualizar componente Vue
                if (vueComponent) {
                    if (vueComponent.$set) {
                        vueComponent.$set(vueComponent, 'value', dateValue);
                        vueComponent.$set(vueComponent, 'displayValue', dateDisplay);
                    }
                    vueComponent.value = dateValue;
                    vueComponent.displayValue = dateDisplay;
                    if (vueComponent.$forceUpdate) vueComponent.$forceUpdate();
                    if (vueComponent.$emit) {
                        vueComponent.$emit('input', dateValue);
                        vueComponent.$emit('change', dateValue);
                    }
                }
                
                // Actualizar input
                const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                nativeSetter.call(input, dateDisplay);
                
                // Disparar eventos
                ['input', 'change', 'blur'].forEach(eventType => {
                    input.dispatchEvent(new Event(eventType, { bubbles: true }));
                });
                
                return true;
            """
            return driver.execute_script(script, input_element, date_value, date_display)
        
        # Aplicar fecha DESDE
        logger.info("Aplicando fecha DESDE...")
        driver.execute_script("arguments[0].click();", input_from_elem)
        sleep(1.0)
        apply_date(input_from_elem, f"{date_from_str} 00:00:00", date_from_str)
        sleep(1.0)
        
        # Cerrar panel presionando ESC
        from selenium.webdriver.common.keys import Keys
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        sleep(1.0)
        
        # Aplicar fecha HASTA
        logger.info("Aplicando fecha HASTA...")
        driver.execute_script("arguments[0].click();", input_to_elem)
        sleep(1.0)
        apply_date(input_to_elem, f"{date_to_str} 23:59:59", date_to_str)
        sleep(1.0)
        
        # Cerrar panel presionando ESC
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        sleep(1.0)
        
        logger.info("Fechas aplicadas correctamente")
    elif settings.date_mode == 2:
        logger.info("Seleccionando rango rápido: Último mes")
        driver.find_element(By.XPATH, '//*[@id="createtimeRow"]/div[2]/div[2]/div/div[1]/label[3]').click()
        sleep(1)
    else:
        raise RuntimeError("DATE_MODE no válido. Usa 1 o 2.")


def _apply_filters(driver) -> None:
    """Simula el hover requerido para que la UI habilite el botón de filtros.

    En Integratel el botón confirma los filtros sólo después del hover sobre el
    split button. ``ActionChains`` reproduce ese comportamiento.
    """
    logger.info("Aplicando filtros (hover + click)...")
    element = driver.find_element(By.CSS_SELECTOR, "#allTask_tab .el-button:nth-child(3)")
    ActionChains(driver).move_to_element(element).perform()
    sleep(2)


def _trigger_export(driver) -> float:
    """Hace click en el botón de exportación y devuelve el timestamp del disparo.

    El timestamp es usado luego por ``_download_export`` para identificar cuál
    de los archivos descargados corresponde al request actual.
    """
    logger.info("Disparando exportación...")
    driver.find_element(By.CSS_SELECTOR, "#test > .sdm_splitbutton_text").click()
    sleep(1)
    return time.time()


def _navigate_to_export_status(iframe_manager: IframeManager) -> None:
    """Cierra el popup de resultados y abre el módulo de estado de exportación.

    Después de lanzar la exportación, Integratel muestra un modal de éxito que
    debe cerrarse. Luego se navega en el menú lateral para entrar a Export
    Status, cambiando de iframe con ``IframeManager`` (teleows.clients.iframes).
    """
    iframe_manager.switch_to_default_content()
    logger.info("📋 Navegando a sección de export status...")
    iframe_manager.driver.find_element(By.CSS_SELECTOR, ".el-icon-close:nth-child(2)").click()
    sleep(1)

    driver = iframe_manager.driver
    driver.find_element(By.CSS_SELECTOR, ".el-row:nth-child(6) > .side-item-icon").click()
    sleep(1)
    driver.find_element(By.CSS_SELECTOR, ".level-1").click()
    sleep(1)
    require(iframe_manager.switch_to_iframe(1), "No se pudo cambiar al iframe de export status")
    sleep(1)


def _monitor_status(driver, timeout_seconds: int, poll_interval: int) -> None:
    """Revisa el panel de exportación hasta que el job finaliza (éxito o error).

    El panel actualiza cada vez que se pulsa el ícono de refresh. Se revisa el
    texto de la primera fila (estado). Al detectar un estado final distinto de
    Succeed se lanza una excepción para que el DAG lo refleje como fallo.
    """
    logger.info("Iniciando monitoreo de estado de exportación...")
    end_states = {"Succeed", "Failed", "Aborted", "Waiting", "Concurrent Waiting"}
    deadline = time.time() + timeout_seconds
    attempt = 0

    while time.time() < deadline:
        attempt += 1
        try:
            driver.find_element(By.CSS_SELECTOR, "span.button_icon.btnIcon[style*='refresh']").click()
            logger.info("Refresh intento %s (restan %.0f s)", attempt, deadline - time.time())
        except Exception as exc:
            logger.warning("⚠ No se pudo presionar Refresh: %s", exc, exc_info=True)

        sleep(2)
        status = driver.find_element(
            By.XPATH, '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]/td[3]/div/span'
        ).text.strip()
        logger.info("Estado de exportación: %s", status)

        if status in end_states:
            if status == "Succeed":
                logger.info("Exportación completada exitosamente")
                return
            raise RuntimeError(f"Proceso de exportación terminó con estado: {status}")

        if status != "Running":
            logger.warning("Estado desconocido '%s'. Continuando monitoreo...", status)

        sleep(poll_interval)

    raise RuntimeError("Tiempo máximo de espera alcanzado durante el monitoreo de exportación")


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
    logger.info("Preparando descarga...")
    before = {p for p in download_dir.iterdir() if p.is_file()}
    driver.find_element(
        By.XPATH, '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]/td[11]/div/div[3]'
    ).click()
    logger.info("✓ Botón de descarga presionado")

    return wait_for_download(
        download_dir,
        since=started_at,
        overwrite=overwrite_files,
        timeout=timeout,
        desired_name=output_filename,
        logger=logger,
        initial_snapshot=tuple(before),
    )


def run_gde(
    settings: TeleowsSettings,
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
        settings:
            Instancia de ``TeleowsSettings`` con credenciales, filtros, rutas y flags.
            Típicamente lo construye el DAG usando ``TeleowsSettings.load_with_overrides``.
        headless / chrome_extra_args:
            Overrides opcionales para el navegador (útiles durante pruebas).
        status_timeout / status_poll_interval:
            Ajustan el tiempo máximo y la frecuencia de refresco al monitorear Export Status.
        output_filename:
            Permite forzar el nombre del archivo final (si no se usa, cae en settings.gde_output_filename).

    Devuelve:
        ``Path`` absoluto del archivo final descargado (renombrado si corresponde).
    """
    download_dir = Path(settings["download_path"]).resolve()
    download_dir.mkdir(parents=True, exist_ok=True)

    browser_kwargs: Dict[str, Any] = {
        "download_path": str(download_dir),
        "headless": settings["headless"] if headless is None else headless,
        "extra_args": chrome_extra_args,
    }
    if settings.proxy:
        browser_kwargs["proxy"] = settings.proxy

    try:
        browser_manager = BrowserManager(**browser_kwargs)
    except TypeError as exc:
        message = str(exc)
        if "unexpected keyword argument 'proxy'" in message and "proxy" in browser_kwargs:
            browser_kwargs.pop("proxy", None)
            logger.warning(
                "⚠ BrowserManager no admite argumento 'proxy' (versión antigua en contenedor). "
                "Continuando sin proxy..."
            )
            browser_manager = BrowserManager(**browser_kwargs)
            if not hasattr(browser_manager, "proxy"):
                browser_manager.proxy = settings.proxy  # type: ignore[attr-defined]
            if settings.proxy:
                os.environ["PROXY"] = settings.proxy
        else:
            raise
    else:
        if not hasattr(browser_manager, "proxy"):
            browser_manager.proxy = settings.proxy  # type: ignore[attr-defined]
        if settings.proxy:
            os.environ["PROXY"] = settings.proxy

    driver, wait = browser_manager.create_driver()

    try:
        auth_manager = AuthManager(driver)
        require(
            auth_manager.login(settings.username, settings.password),
            "No se pudo realizar el login",
        )

        iframe_manager = IframeManager(driver)
        require(
            iframe_manager.find_main_iframe(max_attempts=settings.max_iframe_attempts),
            "No se encontró el iframe principal",
        )

        filter_manager = FilterManager(driver, wait)
        filter_manager.wait_for_filters_ready()
        filter_manager.open_filter_panel(method="complex")

        _click_clear_filters(driver, wait)
        _apply_task_type_filters(driver, wait, settings.options_to_select)

        # Log de debugging para verificar configuración de fechas
        logger.info("🔍 Configuración de fechas: date_mode=%s, last_n_days=%s, date_from=%s, date_to=%s",
                   settings.date_mode, settings.last_n_days, settings.date_from, settings.date_to)

        _apply_date_filters(driver, settings)
        _apply_filters(driver)
        export_started = _trigger_export(driver)

        _navigate_to_export_status(iframe_manager)
        _monitor_status(
            driver,
            status_timeout or settings.max_status_attempts * 30,
            status_poll_interval or 8,
        )

        final_name = (output_filename or settings.gde_output_filename or "").strip() or None
        downloaded = _download_export(
            driver,
            download_dir,
            export_started,
            overwrite_files=settings.export_overwrite_files,
            output_filename=final_name,
        )

        logger.info("🎉 Flujo GDE completado")
        return downloaded
    finally:
        logger.info("ℹ Cerrando navegador...")
        browser_manager.close_driver()


def extraer_gde(
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
    Ejecuta el workflow Selenium de GDE y devuelve la ruta del archivo generado.

    Puede recibir directamente un ``TeleowsSettings`` o, alternativamente,
    construirlo a partir de ``overrides`` (misma convención que EnergiaFacilities).
    """
    effective_settings = settings or load_settings(overrides)
    logger.info(
        "Iniciando extracción GDE (overrides=%s)",
        sorted(overrides.keys()) if overrides else "default",
    )
    path = run_gde(
        effective_settings,
        headless=headless,
        chrome_extra_args=chrome_extra_args,
        status_timeout=status_timeout,
        status_poll_interval=status_poll_interval,
        output_filename=output_filename,
    )
    logger.info("Extracción GDE finalizada. Archivo: %s", path)
    return str(path)


from core.utils import  load_config
config = load_config()
config_autin = config.get("gde", {})

extraer_gde(settings=config_autin)