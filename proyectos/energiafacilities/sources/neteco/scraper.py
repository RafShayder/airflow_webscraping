"""
Script de referencia para NetEco - contiene código de prueba y ejemplos.

Este archivo puede usarse para pruebas rápidas o como referencia.
Para uso en producción, usar stractor.py que contiene el workflow completo.
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
import time
import logging
import os
from typing import Optional

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
from common.selenium_utils import wait_for_download
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
NETECO_PM_VIEW_URL = "https://10.125.129.82:31943/netecopm/pmview/performModule/view/site/pmView.html?curMenuId=com.huawei.neteco.pm.data"
FILTER_BUTTON_CLASS = "nco-search-container-buttons"
# XPath para el elemento de NetEco Monitor
NETECO_MONITOR_XPATH = '//*[@id="refr.mm.NetEco_Monitor"]/span'
NETECO_MONITOR_XPATH_FALLBACK = '/html/body/div[2]/div/div[3]/div/div/div[2]/span[1]/span'


def _resolve_timezone(config_neteco: dict) -> Optional[object]:
    tz_name = (config_neteco.get("timezone") or config_neteco.get("tz") or "America/Lima")
    if ZoneInfo is None:
        logger.warning("zoneinfo no disponible; usando hora local del sistema")
        return None
    try:
        return ZoneInfo(tz_name)
    except Exception:
        logger.warning("Zona horaria invalida '%s'; usando hora local del sistema", tz_name)
        return None

def _build_browser_manager(download_dir: Path, headless: bool) -> BrowserManager:
    cache_dir = Path(os.environ.setdefault("SELENIUM_MANAGER_CACHE_PATH", "/tmp/selenium-cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)
    user_data_dir = f"/tmp/neteco-chrome-{os.getpid()}-{int(time.time())}"
    extra_args = [
        "--ignore-certificate-errors",
        "--allow-insecure-localhost",
        "--ignore-ssl-errors=yes",
        f"--user-data-dir={user_data_dir}",
    ]
    return BrowserManager(
        download_path=str(download_dir),
        headless=headless,
        extra_args=extra_args,
    )

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


def _find_checkbox_by_texts(driver, labels):
    if isinstance(labels, str):
        labels = [labels]
    for label in labels:
        try:
            elements = driver.find_elements(
                By.XPATH,
                f"//*[normalize-space(text())='{label}' or contains(normalize-space(text()), '{label}')]",
            )
        except Exception:
            continue
        for element in elements:
            try:
                if not element.is_displayed():
                    continue
            except Exception:
                continue
            for parent_xpath in ("./..", "./../.."):
                try:
                    container = element.find_element(By.XPATH, parent_xpath)
                except Exception:
                    continue
                try:
                    checkboxes = container.find_elements(
                        By.XPATH,
                        ".//input[@type='checkbox'] | .//*[contains(@class, 'checkbox')] | .//span[contains(@class, 'check')]",
                    )
                except Exception:
                    continue
                for cb in checkboxes:
                    try:
                        if cb.is_displayed():
                            return cb
                    except Exception:
                        continue
    return None


def _resolve_target_filename(directory: Path, desired_name: str, overwrite: bool) -> Path:
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
    field, strategy = _find_task_name_field_in_context(driver)
    if field:
        return field, strategy

    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    for idx, iframe in enumerate(iframes, start=1):
        try:
            driver.switch_to.frame(iframe)
            field, strategy = _find_task_name_field_in_context(driver)
            if field:
                return field, f"iframe[{idx}]::{strategy}"
        except Exception:
            continue
        finally:
            try:
                driver.switch_to.default_content()
            except Exception:
                pass

    return None, None

def _set_datetime_field(driver, xpath, value, label):
    try:
        field = driver.find_element(By.XPATH, xpath)
    except Exception as e:
        logger.debug(f"No se encontró el campo {label}: {e}")
        return

    logger.debug(f"Llenando {label} con {value}...")
    _fill_input(driver, field, value, label=label)


def _click_until_visible(driver, click_xpath, target_xpath, label, max_attempts=5, wait_seconds=1.5):
    for attempt in range(1, max_attempts + 1):
        try:
            target = driver.find_elements(By.XPATH, target_xpath)
            if any(elem.is_displayed() for elem in target):
                logger.debug(f"Objetivo visible antes de click en {label} (intento {attempt})")
                return True
        except Exception:
            pass

        try:
            button = driver.find_element(By.XPATH, click_xpath)
            if button.is_displayed():
                logger.debug(f"Haciendo click en {label} (intento {attempt})")
                _js_click(driver, button)
            else:
                logger.debug(f"Botón {label} no visible (intento {attempt})")
        except Exception as e:
            logger.warning(f"Error haciendo click en {label} (intento {attempt}): {e}")

        time.sleep(wait_seconds)

        try:
            target = driver.find_elements(By.XPATH, target_xpath)
            if any(elem.is_displayed() for elem in target):
                logger.debug(f"Objetivo visible después de click en {label} (intento {attempt})")
                return True
        except Exception:
            pass

    logger.warning(f"No apareció el objetivo después de {max_attempts} intentos: {target_xpath}")
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

def scraper_neteco():
    """
    Función de prueba para login y navegación básica.
    Útil para debugging y pruebas rápidas.
    """
    task_name_value = None
    downloaded_path = None
    driver = None
    browser_manager = None
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
        configured_start_time = config_neteco.get("start_time")
        configured_end_time = config_neteco.get("end_time")
        date_mode = (config_neteco.get("date_mode") or "manual").strip().lower()
        download_button_wait_seconds = int(config_neteco.get("download_button_wait_seconds", 240))
        download_file_timeout_seconds = int(config_neteco.get("download_file_timeout_seconds", 300))
        download_poll_seconds = int(config_neteco.get("download_poll_seconds", 3))
        download_path = config_neteco.get("local_dir") or default_download_path()
        download_dir = Path(download_path).expanduser().resolve()
        download_dir.mkdir(parents=True, exist_ok=True)
        browser_manager = _build_browser_manager(download_dir, headless=headless)
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

        # Navegar a los filtros
        wait = WebDriverWait(driver, 30)
        filtro = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, FILTER_BUTTON_CLASS))
        )
        filtro.click()
        logger.debug("Filtro abierto")
        
        # Hacer hover sobre el elemento de NetEco Monitor para abrir el menú
        monitor_element = None
        try:
            monitor_element = wait.until(
                EC.presence_of_element_located((By.XPATH, NETECO_MONITOR_XPATH))
            )
            logger.debug("Elemento Monitor encontrado (XPath principal)")
        except TimeoutException:
            logger.debug("XPath principal no encontrado, intentando con fallback...")
            try:
                monitor_element = wait.until(
                    EC.presence_of_element_located((By.XPATH, NETECO_MONITOR_XPATH_FALLBACK))
                )
                logger.debug("Elemento Monitor encontrado (XPath fallback)")
            except TimeoutException:
                logger.warning("No se encontró el elemento de NetEco Monitor")
                return
        
        # Hacer hover para mantener el menú abierto
        logger.debug("Haciendo hover sobre Monitor para abrir el menú...")
        ActionChains(driver).move_to_element(monitor_element).perform()
        time.sleep(1)  # Esperar a que el menú se abra
        
        # Buscar y hacer click en "Historical Data"
        logger.debug("Buscando 'Historical Data' en el menú...")
        xpath_strategies = [
            "//*[contains(text(), 'Historical Data')]",
            "//span[contains(text(), 'Historical Data')]",
            "//div[contains(text(), 'Historical Data')]",
            "//a[contains(text(), 'Historical Data')]",
            "//li[contains(text(), 'Historical Data')]",
        ]
        
        historical_data_element = None
        for xpath_strategy in xpath_strategies:
            try:
                historical_data_element = wait.until(
                    EC.element_to_be_clickable((By.XPATH, xpath_strategy))
                )
                logger.debug(f"Elemento encontrado con: {xpath_strategy}")
                break
            except TimeoutException:
                continue
        
        if historical_data_element:
            # Usar JavaScript click para evitar que el menú se cierre
            _js_click(driver, historical_data_element)
            logger.debug("Click realizado en 'Historical Data'")
            time.sleep(2)  # Esperar navegación
        else:
            logger.warning("No se encontró el elemento 'Historical Data'")
            return
        
        # Esperar a que cargue la página de PM View
        logger.debug("Esperando a que cargue la página de PM View...")
        wait.until(EC.url_contains("pmView.html"))
        logger.debug("URL de PM View detectada")
        time.sleep(2)  # Esperar carga completa
        
        # Esperar a que el documento esté completamente cargado
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(1)
        logger.debug("Página cargada completamente")
        
        # Buscar y hacer click en el checkbox "Root"
        root_checkbox = _find_checkbox_by_texts(driver, "Root")
        if root_checkbox:
            logger.debug("Haciendo click en checkbox Root")
            _scroll_into_view(driver, root_checkbox)
            is_checked = driver.execute_script(
                "return arguments[0].checked || arguments[0].getAttribute('aria-checked') === 'true';",
                root_checkbox,
            )
            if not is_checked:
                _js_click(driver, root_checkbox)
                logger.debug("Click realizado en checkbox 'Root'")
                time.sleep(1)
            else:
                logger.debug("Checkbox 'Root' ya estaba marcado")
        else:
            logger.warning("No se encontró el checkbox 'Root'")
            return

        time.sleep(2)

        # Buscar y hacer click en el checkbox "Mains"
        mains_checkbox = _find_checkbox_by_texts(driver, "Mains")
        if mains_checkbox:
            logger.debug("Haciendo click en checkbox 'Mains'")
            _scroll_into_view(driver, mains_checkbox)
            is_checked = driver.execute_script(
                "return arguments[0].checked || arguments[0].getAttribute('aria-checked') === 'true';",
                mains_checkbox,
            )
            if not is_checked:
                _js_click(driver, mains_checkbox)
                logger.debug("Click realizado en checkbox 'Mains'")
                time.sleep(2)
            else:
                logger.debug("Checkbox 'Mains' ya estaba marcado")
                time.sleep(1)
        else:
            logger.warning("No se encontró el checkbox 'Mains'")
            input("Presiona Enter para continuar...")
            return

        time.sleep(6)

        # Buscar y hacer click en el checkbox "(All) Device Instances"
        all_device_types_checkbox = _find_checkbox_by_texts(
            driver, ["(All) Device Instances", "All Device Instances"]
        )
        if all_device_types_checkbox:
            logger.debug("Haciendo click en '(All) Device Instances'")
            _scroll_into_view(driver, all_device_types_checkbox)
            is_checked = driver.execute_script(
                "return arguments[0].checked || arguments[0].getAttribute('aria-checked') === 'true';",
                all_device_types_checkbox,
            )
            if not is_checked:
                _js_click(driver, all_device_types_checkbox)
                logger.debug("Click realizado en checkbox '(All) Device Instances'")
                time.sleep(1)
            else:
                logger.debug("Checkbox '(All) Device Instances' ya estaba marcado")
        else:
            logger.warning("No se encontró el checkbox '(All) Device Instances' en la nueva interfaz")

        # Después de seleccionar (All) Device Instances, buscar y hacer click en el botón Export
        logger.info("Buscando botón Export")
        try:
            # Esperar un poco para que se actualice la interfaz
            time.sleep(2)

            # Buscar botón Export con diferentes estrategias
            export_button = None

            # Estrategia 1: Buscar por texto "Export"
            export_strategies = [
                "//button[contains(text(), 'Export')]",
                "//input[@value='Export']",
                "//a[contains(text(), 'Export')]",
                "//span[contains(text(), 'Export')]",
                "//div[contains(text(), 'Export')]",
                "//*[contains(@class, 'export')]",
                "//button[contains(@title, 'Export')]"
            ]

            for xpath in export_strategies:
                try:
                    buttons = driver.find_elements(By.XPATH, xpath)
                    for button in buttons:
                        if button.is_displayed():
                            export_button = button
                            logger.debug(f"Botón Export encontrado con: {xpath}")
                            break
                    if export_button:
                        break
                except Exception:
                    continue

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
                    logger.warning("No se encontró el campo Task Name en el modal")

                # Llenar Start Time y End Time
                start_time_xpath = "//*[@id='startdatepicker_value']"
                end_time_xpath = "//*[@id='enddatepicker_value']"
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

                _set_datetime_field(driver, start_time_xpath, start_time_value, "Start Time")
                _set_datetime_field(driver, end_time_xpath, end_time_value, "End Time")

                ok_button_xpath = "//*[@id='startDown_button']"
                confirm_button_xpath = "//*[@id='exportConfigWindow']/div[2]/div[2]/span"
                export_confirmed = _click_until_visible(
                    driver,
                    ok_button_xpath,
                    confirm_button_xpath,
                    "OK",
                    max_attempts=6,
                    wait_seconds=1.5,
                )
                if export_confirmed:
                    try:
                        confirm_button = driver.find_element(By.XPATH, confirm_button_xpath)
                        if confirm_button.is_displayed():
                            logger.debug("Haciendo click en botón de confirmación")
                            _js_click(driver, confirm_button)
                    except Exception:
                        logger.warning("No se pudo hacer click en el botón de confirmación")
                else:
                    logger.warning("No apareció el botón de confirmación luego de presionar OK")

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
            logger.debug(f"Error en el proceso de exportación: {e}")

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

    if not downloaded_path:
        raise RuntimeError("No se pudo descargar el archivo NetEco")
    return downloaded_path


if __name__ == "__main__":
    scraper_neteco()
