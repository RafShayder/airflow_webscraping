"""
Scraper de NetEco - Extracción de datos históricos.

Este módulo realiza web scraping del portal NetEco para extraer
datos de Historical Data (PM View) y descargar reportes en formato ZIP.
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta, tzinfo as TzInfo
import time
import logging
import os
import shutil

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

# Configurar path para imports
current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[2]))  # /.../energiafacilities
sys.path.insert(0, str(current_path.parents[3]))  # /.../proyectos
sys.path.insert(0, str(current_path.parents[4]))  # repo root

from core.utils import load_config
from core.helpers import default_download_path
from common.selenium_utils import wait_for_download, _resolve_target_filename
from clients.browser import BrowserManager
from clients.auth import AuthManager, LoginSelectors
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException

DEBUG_LOGS = False
logging.basicConfig(
    level=logging.DEBUG if DEBUG_LOGS else logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# URL de NetEco
NETECO_BASE_URL = "https://10.125.129.82:31943"
NETECO_SUCCESS_URL = "https://10.125.129.82:31943/netecodashboard/assets/build/index.html#/dashboard/overview"

# IDs específicos de checkboxes en el árbol de selección (ZTree)
CHECKBOX_ROOT_ID = "deviceTree_element_1_check"
CHECKBOX_MAINS_ID = "deviceTypesTree_element_4_check"
CHECKBOX_ALL_INSTANCES_ID = "instanceTree_element_1_check"


def _resolve_timezone(config_neteco: dict) -> TzInfo | None:
    tz_name = (config_neteco.get("timezone") or config_neteco.get("tz") or "America/Lima")
    if ZoneInfo is None:
        logger.warning("zoneinfo no disponible; usando hora local del sistema")
        return None
    try:
        return ZoneInfo(tz_name)
    except Exception:
        logger.warning("Zona horaria invalida '%s'; usando hora local del sistema", tz_name)
        return None

def _build_browser_manager(download_dir: Path, headless: bool) -> tuple[BrowserManager, str]:
    cache_dir = Path(os.environ.setdefault("SELENIUM_MANAGER_CACHE_PATH", "/tmp/selenium-cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)
    user_data_dir = f"/tmp/neteco-chrome-{os.getpid()}-{int(time.time())}"
    extra_args = [
        "--ignore-certificate-errors",
        "--allow-insecure-localhost",
        "--ignore-ssl-errors=yes",
        f"--user-data-dir={user_data_dir}",
    ]
    browser_manager = BrowserManager(
        download_path=str(download_dir),
        headless=headless,
        extra_args=extra_args,
    )
    return browser_manager, user_data_dir

def _scroll_into_view(driver, element, pause_seconds=0.2):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        if pause_seconds:
            time.sleep(pause_seconds)
    except Exception:
        pass


def _js_click(driver, element, pause_seconds=0.2):
    _scroll_into_view(driver, element, pause_seconds=pause_seconds)
    try:
        driver.execute_script("arguments[0].click();", element)
    except Exception:
        pass


def _fill_input(driver, field, value, label=None):
    _scroll_into_view(driver, field)
    try:
        field.click()
    except Exception:
        pass
    try:
        field.clear()
    except Exception:
        pass
    try:
        field.send_keys(value)
    except Exception:
        pass
    try:
        driver.execute_script(
            "arguments[0].value = arguments[1];"
            "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));"
            "arguments[0].dispatchEvent(new Event('change', {bubbles: true}));",
            field,
            value,
        )
    except Exception:
        pass
    if label:
        try:
            filled_value = field.get_attribute("value")
            logger.debug(f"Valor actual en {label}: {filled_value!r}")
        except Exception:
            pass


def _click_checkbox_by_id(driver, element_id: str, label: str, wait_seconds: float = 2.0) -> bool:
    """
    Hace click en un checkbox usando su ID específico.
    Retorna True si el click fue exitoso, False si no se encontró el elemento.
    """
    wait = WebDriverWait(driver, wait_seconds)
    try:
        checkbox = wait.until(EC.presence_of_element_located((By.ID, element_id)))
        _scroll_into_view(driver, checkbox)
        is_checked = driver.execute_script(
            "return arguments[0].checked || arguments[0].getAttribute('aria-checked') === 'true';",
            checkbox,
        )
        if not is_checked:
            _js_click(driver, checkbox)
            logger.debug(f"Click realizado en checkbox '{label}'")
        else:
            logger.debug(f"Checkbox '{label}' ya estaba marcado")
        return True
    except TimeoutException:
        logger.error(f"No se encontró el checkbox '{label}' con ID: {element_id}")
        return False
    except Exception as e:
        logger.error(f"Error al hacer click en checkbox '{label}': {e}")
        return False


def _rename_downloaded(downloaded: Path, desired_name: str, overwrite: bool) -> Path:
    target = _resolve_target_filename(downloaded.parent, desired_name, overwrite)
    if overwrite and target.exists():
        target.unlink()
    downloaded.rename(target)
    return target

def _find_task_name_field_in_context(driver):
    xpath = "//input[@id='fileName_value']"
    try:
        fields = driver.find_elements(By.XPATH, xpath)
    except Exception:
        return None, None
    for field in fields:
        try:
            if field.is_displayed() and field.is_enabled():
                return field, xpath
        except Exception:
            continue
    return None, None


def _find_task_name_field(driver):
    """Busca el campo Task Name en el modal de exportación."""
    return _find_task_name_field_in_context(driver)

def _set_datetime_field(driver, xpath, value, label):
    try:
        field = driver.find_element(By.XPATH, xpath)
    except Exception as e:
        logger.warning(f"No se encontró el campo {label}: {e}")
        return False

    logger.info(f"Llenando {label} con valor: {value}")

    # Intentar limpiar y llenar el campo
    _scroll_into_view(driver, field)
    try:
        field.click()
        time.sleep(0.3)
    except Exception:
        pass

    # Limpiar el campo existente
    try:
        field.clear()
    except Exception:
        pass

    # Intentar con JavaScript para asegurar que se establece el valor
    try:
        driver.execute_script(
            """
            var field = arguments[0];
            var value = arguments[1];
            field.value = value;
            field.dispatchEvent(new Event('input', {bubbles: true}));
            field.dispatchEvent(new Event('change', {bubbles: true}));
            field.dispatchEvent(new Event('blur', {bubbles: true}));
            """,
            field,
            value,
        )
    except Exception as e:
        logger.warning(f"Error al establecer {label} via JS: {e}")

    # También intentar con send_keys
    try:
        field.clear()
        field.send_keys(value)
    except Exception:
        pass

    # Verificar que el valor se estableció correctamente
    time.sleep(0.5)
    try:
        actual_value = field.get_attribute("value")
        if actual_value == value:
            logger.info(f"{label} establecido correctamente: {actual_value}")
            return True
        else:
            logger.warning(f"{label} tiene valor diferente. Esperado: {value!r}, Actual: {actual_value!r}")
            return False
    except Exception as e:
        logger.warning(f"No se pudo verificar {label}: {e}")
        return False


def _wait_and_click_download(
    driver,
    task_name,
    download_dir: Path,
    *,
    max_wait_seconds=10800,
    poll_seconds=2,
    download_timeout_seconds=1200,
    overwrite=True,
    desired_filename: str | None = None,
):
    deadline = time.time() + max_wait_seconds
    table_xpath = "//*[@id='taskListWindow']/div[1]/div/table"
    row_xpath = ".//tbody/tr[contains(@ng-repeat, 'downMsg') or contains(@class, 'ng-scope')]"
    download_xpath = (
        ".//td[3]//span[contains(@ng-click, 'downFileEvent') "
        "or contains(@title, 'Download') or normalize-space(text())='Download']"
    )
    loading_xpath = ".//td[3]//span[contains(@ng-if, \"msg.fileType=='creating'\")]//img[contains(@src, 'progress')]"

    while time.time() < deadline:
        contexts = [None]
        try:
            contexts.extend(driver.find_elements(By.TAG_NAME, "iframe"))
        except Exception:
            pass

        found_any_rows = False
        found_task = False

        for iframe in contexts:
            try:
                if iframe is None:
                    driver.switch_to.default_content()
                else:
                    driver.switch_to.frame(iframe)
            except Exception:
                continue

            try:
                table = driver.find_element(By.XPATH, table_xpath)
                rows = table.find_elements(By.XPATH, row_xpath)
            except Exception:
                rows = []

            if rows:
                found_any_rows = True

            for idx, row in enumerate(rows, start=1):
                try:
                    task_cell = row.find_element(By.XPATH, "./td[1]//span")
                    task_text = task_cell.text.strip()
                except Exception:
                    continue

                if task_name != task_text:
                    continue

                found_task = True

                try:
                    download_button = row.find_element(By.XPATH, download_xpath)
                except Exception:
                    try:
                        loading = row.find_elements(By.XPATH, loading_xpath)
                    except Exception:
                        loading = []
                    try:
                        status_text = row.find_element(By.XPATH, "./td[3]").text.strip()
                    except Exception:
                        status_text = ""
                    if loading:
                        logger.debug(f"Task {task_name} sigue en estado creando (fila {idx})")
                    elif status_text:
                        logger.debug(f"Download aún no disponible para {task_name} (fila {idx}): {status_text}")
                    else:
                        logger.debug(f"Download aún no disponible para {task_name} (fila {idx})")
                    break

                try:
                    status_text = ""
                    try:
                        status_text = row.find_element(By.XPATH, "./td[3]").text.strip()
                    except Exception:
                        pass
                    is_disabled = (
                        "disabled" in (download_button.get_attribute("class") or "").lower()
                        or download_button.get_attribute("disabled") is not None
                        or (download_button.get_attribute("aria-disabled") or "").lower() == "true"
                    )
                    if is_disabled:
                        if status_text:
                            logger.debug(f"Download aún no disponible para {task_name} (fila {idx}): {status_text}")
                        else:
                            logger.debug(f"Download aún no disponible para {task_name} (fila {idx})")
                        break

                    logger.debug(f"Haciendo click en Download para {task_name} (fila {idx})")
                    click_time = time.time()
                    _js_click(driver, download_button)
                    try:
                        downloaded = wait_for_download(
                            download_dir,
                            since=click_time,
                            overwrite=overwrite,
                            timeout=download_timeout_seconds,
                            desired_name=desired_filename if desired_filename and Path(desired_filename).suffix else None,
                            logger=logger,
                        )
                        if desired_filename and not Path(desired_filename).suffix:
                            desired_name_with_suffix = f"{desired_filename}{downloaded.suffix}"
                            downloaded = _rename_downloaded(
                                downloaded, desired_name_with_suffix, overwrite=overwrite
                            )
                        logger.info(f"Archivo descargado: {downloaded.name}")
                    except Exception as e:
                        logger.warning(f"No se pudo detectar/renombrar descarga para {task_name}: {e}")
                        return None
                    return downloaded
                except Exception as e:
                    logger.warning(f"Error al intentar click en Download para {task_name}: {e}")
                    return None

            if found_task:
                break

        try:
            driver.switch_to.default_content()
        except Exception:
            pass

        if not found_any_rows:
            logger.debug("Esperando que aparezca la tabla de tareas...")
        elif not found_task:
            logger.debug(f"Task {task_name!r} aún no aparece en la tabla")

        time.sleep(poll_seconds)

    logger.warning(f"Timeout esperando Download para {task_name}")
    return None

def scraper_neteco(
    date_mode_override: str | None = None,
    start_time_override: str | None = None,
    end_time_override: str | None = None,
):
    """
    Scraper de NetEco para extracción de datos históricos.

    Args:
        date_mode_override: Sobrescribe el modo de fecha del config ("auto" o "manual")
        start_time_override: Sobrescribe la fecha de inicio (formato: YYYY-MM-DD HH:MM:SS)
        end_time_override: Sobrescribe la fecha de fin (formato: YYYY-MM-DD HH:MM:SS)

    Returns:
        Path al archivo descargado
    """
    task_name_value = None
    downloaded_path = None
    driver = None
    browser_manager = None
    chrome_user_data_dir = None
    try:
        config = load_config()
        config_neteco = config.get("neteco", {})
        usuario = config_neteco["username"]
        password = config_neteco["password"]
        overwrite_download = config_neteco.get(
            "export_overwrite_files", config_neteco.get("overwrite_download", True)
        )
        desired_filename = (
            config_neteco.get("specific_filename")
            or config_neteco.get("neteco_output_filename")
            or None
        )
        headless = bool(config_neteco.get("headless", False))

        # Usar overrides si se proporcionan, sino usar config
        configured_start_time = start_time_override or config_neteco.get("start_time")
        configured_end_time = end_time_override or config_neteco.get("end_time")
        date_mode = (date_mode_override or config_neteco.get("date_mode") or "manual").strip().lower()

        if date_mode_override:
            logger.info(f"Usando date_mode desde parámetro DAG: {date_mode}")
        if start_time_override or end_time_override:
            logger.info(f"Usando fechas desde parámetros DAG: {configured_start_time} - {configured_end_time}")

        # Validar rango de fechas antes de iniciar el navegador
        if date_mode == "auto":
            tzinfo = _resolve_timezone(config_neteco)
            now_ref = datetime.now(tzinfo) if tzinfo else datetime.now()
            yesterday = now_ref - timedelta(days=1)
            start_time_value = yesterday.replace(hour=0, minute=0, second=1, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            end_time_value = yesterday.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        elif configured_start_time and configured_end_time:
            start_time_value = configured_start_time
            end_time_value = configured_end_time
        else:
            raise ValueError("Faltan start_time/end_time para modo manual en NetEco")

        # Validar que el rango no exceda 31 días
        start_dt = datetime.strptime(start_time_value, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_time_value, "%Y-%m-%d %H:%M:%S")
        date_diff = (end_dt - start_dt).days
        if date_diff > 31:
            raise ValueError(
                f"El rango de fechas excede el límite de 31 días. "
                f"Rango actual: {date_diff} días ({start_time_value} a {end_time_value})"
            )
        logger.info(f"Rango de fechas válido: {start_time_value} a {end_time_value} ({date_diff} días)")

        download_button_wait_seconds = int(config_neteco.get("download_button_wait_seconds", 240))
        download_file_timeout_seconds = int(config_neteco.get("download_file_timeout_seconds", 300))
        download_poll_seconds = int(config_neteco.get("download_poll_seconds", 3))
        download_path = config_neteco.get("local_dir") or default_download_path()
        download_dir = Path(download_path).expanduser().resolve()
        download_dir.mkdir(parents=True, exist_ok=True)
        browser_manager, chrome_user_data_dir = _build_browser_manager(download_dir, headless=headless)
        driver, _ = browser_manager.create_driver()
        logger.info("Iniciando flujo NetEco")

        selectors = LoginSelectors(
            username=(By.ID, "username"),
            password=(By.ID, "value"),
            submit=(By.ID, "submitDataverify"),
        )

        auth = AuthManager(
            driver=driver,
            login_url=NETECO_BASE_URL,
            success_url_contains=NETECO_SUCCESS_URL,
            selectors=selectors,
        )

        auth.login(usuario, password)
        logger.info("Login completado")

        wait = WebDriverWait(driver, 30)

        # Esperar a que cargue el dashboard completamente
        logger.debug("Esperando carga del dashboard...")
        wait.until(EC.url_contains("dashboard"))
        time.sleep(3)

        # Navegar a Historical Data: hover sobre "Monitor" y click en "Historical Data"
        logger.debug("Navegando a Historical Data...")

        # Buscar "Monitor" en la barra de navegación superior (puede ser div o span)
        monitor_element = wait.until(
            EC.presence_of_element_located((By.XPATH, "//*[normalize-space(text())='Monitor']"))
        )
        logger.debug("Elemento 'Monitor' encontrado")

        # Hacer hover sobre Monitor para abrir el submenú
        ActionChains(driver).move_to_element(monitor_element).perform()
        time.sleep(1.5)

        # Esperar y click en "Historical Data" (es un link <a>)
        historical_data_link = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Historical Data"))
        )
        _js_click(driver, historical_data_link)
        logger.debug("Click realizado en 'Historical Data'")

        # Esperar a que cargue la página de PM View
        wait.until(EC.url_contains("pmView.html"))
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(2)
        logger.debug("Página PM View cargada")

        # Seleccionar checkboxes usando IDs específicos (ZTree components)
        # Paso 1: Seleccionar "Root" en la primera columna (Sitios)
        if not _click_checkbox_by_id(driver, CHECKBOX_ROOT_ID, "Root", wait_seconds=10):
            raise RuntimeError(f"No se encontró el checkbox 'Root' con ID: {CHECKBOX_ROOT_ID}")
        time.sleep(2)

        # Paso 2: Seleccionar "Mains" en la segunda columna (Device Types)
        if not _click_checkbox_by_id(driver, CHECKBOX_MAINS_ID, "Mains", wait_seconds=10):
            raise RuntimeError(f"No se encontró el checkbox 'Mains' con ID: {CHECKBOX_MAINS_ID}")
        time.sleep(2)

        # Paso 3: Seleccionar "(All) Device Instances" en la tercera columna
        if not _click_checkbox_by_id(driver, CHECKBOX_ALL_INSTANCES_ID, "(All) Device Instances", wait_seconds=10):
            logger.warning(f"No se encontró el checkbox '(All) Device Instances' con ID: {CHECKBOX_ALL_INSTANCES_ID}")

        # Click en el botón Export
        logger.info("Buscando botón Export")
        try:
            time.sleep(2)

            # Buscar botón Export por role (más estable que XPath)
            export_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Export')]"))
            )

            if export_button:
                # Hacer click en el botón Export
                logger.info("Haciendo click en botón Export")
                _js_click(driver, export_button, pause_seconds=0.5)
                logger.debug("Click realizado en botón Export")

                # Esperar a que aparezca el modal
                logger.debug("Esperando a que aparezca el modal...")
                time.sleep(3)

                # Buscar el campo Task Name en el modal
                logger.debug("Buscando campo Task Name...")
                task_name_field = None

                task_name_field, task_name_strategy = _find_task_name_field(driver)
                if task_name_field:
                    logger.debug(f"Campo Task Name encontrado con: {task_name_strategy}")

                if task_name_field:
                    task_name_value = f"NETECO_{datetime.now().strftime('%d%m%Y%H%M%S')}"
                    logger.debug(f"Llenando campo Task Name con {task_name_value!r}")
                    _fill_input(driver, task_name_field, task_name_value, label="Task Name")
                    logger.debug("Intento de llenado del campo Task Name completado")
                else:
                    logger.error("No se encontró el campo Task Name en el modal de exportación")
                    raise RuntimeError("No se encontró el campo Task Name en el modal de exportación")

                # Llenar Start Time y End Time (ya validados al inicio)
                start_time_xpath = "//*[@id='startdatepicker_value']"
                end_time_xpath = "//*[@id='enddatepicker_value']"

                start_ok = _set_datetime_field(driver, start_time_xpath, start_time_value, "Start Time")
                end_ok = _set_datetime_field(driver, end_time_xpath, end_time_value, "End Time")

                if not start_ok or not end_ok:
                    logger.warning("Los campos de fecha pueden no haberse llenado correctamente. Continuando de todos modos...")

                # Esperar un momento para que la UI procese los cambios
                time.sleep(1)

                # Click en el botón OK del modal "Create Export Task"
                # Buscar el botón OK por texto dentro del contenedor del modal
                ok_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//*[@id='startDown_button']//*[normalize-space(text())='OK']"))
                )
                logger.info("Haciendo click en botón OK")
                _js_click(driver, ok_button)
                time.sleep(2)

                # Después de OK aparece un modal de confirmación - click en "View Task"
                # Este botón abre la tabla de tareas donde esperamos el Download
                try:
                    view_task_button = wait.until(
                        EC.element_to_be_clickable((By.XPATH, "//*[normalize-space(text())='View Task']"))
                    )
                    logger.info("Haciendo click en 'View Task'")
                    _js_click(driver, view_task_button)
                    time.sleep(1)
                except TimeoutException:
                    logger.warning("No apareció el botón 'View Task', la tabla de descargas puede ya estar visible")

                if task_name_value:
                    logger.info(f"Esperando Download para {task_name_value!r}")
                    try:
                        downloaded_path = _wait_and_click_download(
                            driver,
                            task_name_value,
                            download_dir,
                            overwrite=overwrite_download,
                            desired_filename=desired_filename,
                            max_wait_seconds=download_button_wait_seconds,
                            poll_seconds=download_poll_seconds,
                            download_timeout_seconds=download_file_timeout_seconds,
                        )
                    except Exception as e:
                        logger.warning(f"Error esperando Download para {task_name_value!r}: {e}")

            else:
                logger.warning("No se encontró el botón Export")

        except Exception as e:
            logger.error(f"Error en el proceso de exportación: {e}")

        logger.info("Proceso completado.")
        if task_name_value:
            logger.debug(f"Secuencia finalizada: Root → Mains → (All) Device Instances → Export → Task Name: {task_name_value!r}")
        else:
            logger.debug("Secuencia finalizada: Root → Mains → (All) Device Instances → Export → Task Name: no definido")
        # input("Presiona Enter para continuar...")  # Deshabilitado para ejecución automática
    finally:
        if browser_manager is not None:
            browser_manager.close_driver()
        elif driver is not None:
            driver.quit()
        # Limpiar directorio temporal de Chrome para evitar acumulación
        if chrome_user_data_dir:
            try:
                shutil.rmtree(chrome_user_data_dir, ignore_errors=True)
                logger.debug(f"Directorio temporal de Chrome eliminado: {chrome_user_data_dir}")
            except Exception:
                pass

    if not downloaded_path:
        logger.error("No se pudo descargar el archivo NetEco")
        raise RuntimeError("No se pudo descargar el archivo NetEco")
    return downloaded_path


if __name__ == "__main__":
    scraper_neteco()
