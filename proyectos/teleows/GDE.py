import logging
import os
import time
from pathlib import Path
from time import sleep

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from .config import (
    DATE_FROM,
    DATE_MODE,
    DATE_TO,
    DOWNLOAD_PATH,
    MAX_IFRAME_ATTEMPTS,
    MAX_STATUS_ATTEMPTS,
    OPTIONS_TO_SELECT,
    PASSWORD,
    USERNAME,
    GDE_OUTPUT_FILENAME,
    EXPORT_OVERWRITE_FILES,
)
from .core.auth_manager import AuthManager
from .core.browser_manager import BrowserManager
from .core.filter_manager import FilterManager
from .core.iframe_manager import IframeManager


# Automatiza la navegación y descarga del reporte GDE desde la plataforma web.
# El módulo se divide en pequeños pasos reutilizables para que cada acción sea clara.
logger = logging.getLogger(__name__)


def require(condition, message):
    """Utilidad para garantizar requisitos obligatorios del flujo."""
    if not condition:
        raise RuntimeError(message)


def _click_clear_filters(driver, wait):
    # Evitamos que filtros antiguos interfieran con el resultado limpiándolos explícitamente.
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="allTask_tab"]/form/div[2]/div/div/div[2]/button[2]')
        )
    ).click()
    logger.info("✓ Filtros anteriores limpiados")
    sleep(1)


def _apply_task_type_filters(driver, wait, options):
    logger.info("📋 Asignando opciones en Task Type...")
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#all_taskType .el-select__caret"))).click()
    sleep(1)

    # Seleccionamos cada elemento solicitado dentro del menú desplegable de Task Type.
    for option in options:
        xpath = f"//li[contains(@class, 'el-select-dropdown__item') and @title='{option}']"
        wait.until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
        logger.info("✓ %s", option)
        sleep(0.3)


def _apply_date_filters(driver):
    # Dependiendo de la configuración global, insertamos fechas manuales o usamos un rango predefinido.
    if DATE_MODE == 1:
        logger.info("📅 Aplicando fechas manuales: %s → %s", DATE_FROM, DATE_TO)
        script_from = f"""
            const xpath = '//*[@id="closetimeRow"]/div[2]/div[2]/div/div/div[2]/div[1]/input';
            const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
            const input = result.singleNodeValue;

            if (input) {{
                input.value = "{DATE_FROM}";
                input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                return true;
            }}
            return false;
        """
        script_to = f"""
            const xpath = '//*[@id="closetimeRow"]/div[2]/div[3]/div/div/div[2]/div[1]/input';
            const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
            const input = result.singleNodeValue;

            if (input) {{
                input.value = "{DATE_TO}";
                input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                input.dispatchEvent(new Event('change', {{ bubbles: true }}));
                return true;
            }}
            return false;
        """
        # Ejecutamos JavaScript para llenar ambos inputs y disparar los eventos que la UI necesita.
        require(driver.execute_script(script_from), "No se pudo aplicar la fecha DESDE")
        require(driver.execute_script(script_to), "No se pudo aplicar la fecha HASTA")
        sleep(0.5)
    elif DATE_MODE == 2:
        logger.info("📅 Seleccionando rango rápido: Último mes")
        driver.find_element(By.XPATH, '//*[@id="createtimeRow"]/div[2]/div[2]/div/div[1]/label[3]').click()
        sleep(1)
    else:
        raise RuntimeError("DATE_MODE no válido. Usa 1 o 2.")


def _apply_filters(driver):
    logger.info("🔧 Aplicando filtros...")
    # El botón real se activa al posar el mouse; ActionChains reproduce ese gesto humano.
    element = driver.find_element(By.CSS_SELECTOR, "#allTask_tab .el-button:nth-child(3)")
    ActionChains(driver).move_to_element(element).perform()
    sleep(2)


def _trigger_export(driver):
    logger.info("📤 Disparando exportación...")
    driver.find_element(By.CSS_SELECTOR, "#test > .sdm_splitbutton_text").click()
    sleep(1)
    # Guardamos el momento del click para detectar el archivo correcto en la carpeta de descargas.
    return time.time()


def _navigate_to_export_status(iframe_manager):
    iframe_manager.switch_to_default_content()
    logger.info("📋 Navegando a sección de export status...")

    # Cerramos el modal de resultados para recuperar acceso al menú lateral.
    iframe_manager.driver.find_element(By.CSS_SELECTOR, ".el-icon-close:nth-child(2)").click()
    sleep(1)

    driver = iframe_manager.driver
    # La navegación lateral requiere expandir el grupo y luego pulsar la opción deseada.
    driver.find_element(By.CSS_SELECTOR, ".el-row:nth-child(6) > .side-item-icon").click()
    sleep(1)
    driver.find_element(By.CSS_SELECTOR, ".level-1").click()
    sleep(1)

    # La tabla de estados se renderiza en un iframe distinto: cambiamos el foco antes de seguir.
    require(iframe_manager.switch_to_iframe(1), "No se pudo cambiar al iframe de export status")
    sleep(1)


def _monitor_status(driver, timeout_seconds, poll_interval):
    logger.info("🔄 Iniciando monitoreo de estado de exportación...")

    end_states = {"Succeed", "Failed", "Aborted", "Waiting", "Concurrent Waiting"}

    deadline = time.time() + timeout_seconds
    attempt = 0

    # Volvemos a consultar la tabla hasta que el proceso termine o se alcance el timeout.
    while time.time() < deadline:
        attempt += 1
        try:
            driver.find_element(By.CSS_SELECTOR, "span.button_icon.btnIcon[style*='refresh']").click()
            logger.info("🔄 Refresh intento %s (restan %.0f s)", attempt, deadline - time.time())
        except Exception as exc:
            logger.warning("⚠ No se pudo presionar Refresh: %s", exc, exc_info=True)

        sleep(2)

        status = driver.find_element(
            By.XPATH, '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]/td[3]/div/span'
        ).text.strip()
        logger.info("📊 Estado de exportación: %s", status)

        if status in end_states:
            if status == "Succeed":
                logger.info("✅ Exportación completada exitosamente")
                return
            raise RuntimeError(f"Proceso de exportación terminó con estado: {status}")

        if status != "Running":
            logger.warning("⚠ Estado desconocido '%s'. Continuando monitoreo...", status)

        sleep(poll_interval)

    raise RuntimeError("Tiempo máximo de espera alcanzado durante el monitoreo de exportación")


def _resolve_target_filename(directory, desired_name, overwrite):
    target = directory / Path(desired_name).name
    if overwrite or not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    counter = 1
    while True:
        candidate = target.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def _download_export(driver, download_dir, started_at, timeout=120, output_filename=None):
    logger.info("📥 Preparando descarga...")
    before = {p for p in download_dir.iterdir() if p.is_file()}
    driver.find_element(
        By.XPATH, '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]/td[11]/div/div[3]'
    ).click()
    logger.info("✓ Botón de descarga presionado")

    deadline = time.time() + timeout

    # Detectamos el nuevo archivo comparando el listado anterior con el actual.
    while time.time() < deadline:
        current = {p for p in download_dir.iterdir() if p.is_file()}
        candidates = [
            p for p in current - before if not p.name.endswith(".crdownload") and p.stat().st_mtime >= started_at
        ]
        if candidates:
            latest = max(candidates, key=lambda p: p.stat().st_mtime)
            logger.info("✅ Descarga detectada: %s", latest.name)
            if output_filename:
                target_path = _resolve_target_filename(download_dir, output_filename, EXPORT_OVERWRITE_FILES)
                try:
                    if EXPORT_OVERWRITE_FILES and target_path.exists():
                        target_path.unlink()
                    latest.rename(target_path)
                    latest = target_path
                    logger.info("📦 Archivo renombrado a: %s", target_path.name)
                except Exception as exc:
                    message = f"No se pudo renombrar el archivo descargado a {target_path.name}"
                    logger.error("❌ %s", message, exc_info=True)
                    raise RuntimeError(message) from exc
            return latest
        sleep(2)

    raise RuntimeError("No se detectó la descarga dentro del tiempo límite")


def run_gde(
    *,
    download_path=DOWNLOAD_PATH,
    headless=False,
    chrome_extra_args=None,
    status_timeout=600,
    status_poll_interval=8,
    output_filename=None,
):
    """Ejecuta el flujo completo de exportación para GDE y devuelve el archivo descargado."""
    download_dir = Path(download_path).resolve()
    download_dir.mkdir(parents=True, exist_ok=True)

    # Configuramos el navegador según las preferencias recibidas (descargas, headless, flags extra).
    browser_manager = BrowserManager(
        download_path=str(download_dir),
        headless=headless,
        extra_args=chrome_extra_args,
    )
    driver, wait = browser_manager.create_driver()

    try:
        # 1) Autenticación y preparación básica de la interfaz.
        auth_manager = AuthManager(driver)
        require(auth_manager.login(USERNAME, PASSWORD), "No se pudo realizar el login")

        iframe_manager = IframeManager(driver)
        require(
            iframe_manager.find_main_iframe(max_attempts=MAX_IFRAME_ATTEMPTS),
            "No se encontró el iframe principal",
        )

        filter_manager = FilterManager(driver, wait)
        filter_manager.wait_for_filters_ready()
        filter_manager.open_filter_panel(method="complex")

        # 2) Configuración de filtros de negocio antes de lanzar la exportación.
        _click_clear_filters(driver, wait)
        _apply_task_type_filters(driver, wait, OPTIONS_TO_SELECT)
        _apply_date_filters(driver)
        _apply_filters(driver)
        export_started = _trigger_export(driver)

        # 3) Seguimiento del job de exportación y descarga del archivo resultante.
        _navigate_to_export_status(iframe_manager)
        _monitor_status(driver, status_timeout, status_poll_interval)
        final_name = (output_filename or GDE_OUTPUT_FILENAME or "").strip() or None
        downloaded = _download_export(driver, download_dir, export_started, output_filename=final_name)

        logger.info("🎉 Flujo GDE completado")
        return downloaded

    finally:
        logger.info("ℹ Cerrando navegador...")
        browser_manager.close_driver()


def prueba():
    return run_gde(headless=True)
