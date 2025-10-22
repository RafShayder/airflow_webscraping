"""
Script para navegar a Dynamic checklist > Sub PM Query
Reutiliza el AuthManager y navega a la sección específica
"""

import logging
import time
from pathlib import Path
from time import sleep

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from config import (
    DOWNLOAD_PATH,
    MAX_IFRAME_ATTEMPTS,
    MAX_STATUS_ATTEMPTS,
    PASSWORD,
    USERNAME,
)
from src.auth_manager import AuthManager
from src.browser_manager import BrowserManager
from src.filter_manager import FilterManager
from src.iframe_manager import IframeManager


logger = logging.getLogger(__name__)


def require(condition, message):
    """Utilidad para detener el flujo cuando una condición obligatoria no se cumple."""
    if not condition:
        raise RuntimeError(message)


def navigate_to_menu_item(driver, wait, menu_index, item_title, item_name):
    try:
        menu_items = wait.until(lambda d: d.find_elements(By.CSS_SELECTOR, ".menu-item.sideItem"))
        logger.info("ℹ Encontrados %s elementos del menú", len(menu_items))

        if len(menu_items) <= menu_index:
            message = f"No se encontraron suficientes elementos del menú. Encontrados: {len(menu_items)}"
            logger.error("❌ %s", message)
            raise RuntimeError(message)

        target_menu_item = menu_items[menu_index]
        ActionChains(driver).move_to_element(target_menu_item).perform()
        logger.info("✓ Hover realizado sobre el elemento del menú (índice %s)", menu_index)
        sleep(1)

        wait.until(EC.element_to_be_clickable((By.XPATH, f"//span[@title='{item_title}']"))).click()
        logger.info("✓ %s seleccionado", item_name)
        sleep(2)
        return True

    except Exception as exc:
        message = f"Error al seleccionar {item_name}"
        logger.error("❌ %s", message, exc_info=True)
        raise RuntimeError(message) from exc


def navigate_to_submenu_item(wait, submenu_xpath, submenu_name):
    try:
        wait.until(EC.element_to_be_clickable((By.XPATH, submenu_xpath))).click()
        logger.info("✓ %s seleccionado", submenu_name)
        sleep(3)
        return True
    except Exception as exc:
        message = f"Error al seleccionar {submenu_name}"
        logger.error("❌ %s", message, exc_info=True)
        raise RuntimeError(message) from exc


def monitor_export_loader(driver):
    logger.info("⏳ Esperando a que termine la exportación...")
    logger.info("ℹ Nota: La exportación puede tardar hasta 5 minutos, por favor espere...")
    logger.info("🔍 Monitoreando: loader desaparece O mensaje de aviso aparece...")

    case_detected = None
    start_time = time.time()

    while time.time() - start_time < 300:
        try:
            info_message = driver.find_elements(
                By.XPATH, "//div[@class='prompt-window']//span[contains(text(),'Se ha tardado 60 segundos')]"
            )
            if info_message:
                case_detected = "log_management"
                logger.info("✓ Caso 2 detectado: Mensaje de aviso apareció - navegar a Log Management")
                break

            loader_present = driver.find_elements(
                By.XPATH, "//p[@class='el-loading-text' and contains(text(),'Exportando')]"
            )
            if not loader_present:
                logger.info("ℹ Loader desapareció - verificando si aparece mensaje modal...")
                additional_start = time.time()

                while time.time() - additional_start < 10:
                    info_message = driver.find_elements(
                        By.XPATH, "//div[@class='prompt-window']//span[contains(text(),'Se ha tardado 60 segundos')]"
                    )
                    if info_message:
                        case_detected = "log_management"
                        logger.info(
                            "✓ Caso 2 detectado: Mensaje de aviso apareció después del loader - navegar a Log Management"
                        )
                        break
                    sleep(1)

                if not case_detected:
                    case_detected = "direct_download"
                    logger.info("✓ Caso 1 confirmado: Loader desapareció y no apareció mensaje - descarga directa")

                break

            sleep(2)

        except Exception:
            logger.exception("⚠ Error durante monitoreo de exportación")
            sleep(2)

    if not case_detected:
        logger.warning("⏱️ Timeout: No se detectó ningún caso después de 5 minutos")

    return case_detected


class DynamicChecklistWorkflow:
    def __init__(self, driver, wait, download_path=DOWNLOAD_PATH):
        self.driver = driver
        self.wait = wait
        self.iframe_manager = IframeManager(driver)
        self.filter_manager = FilterManager(driver, wait)
        self.download_dir = Path(download_path).resolve()
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.run_start = time.time()

    def run(self):
        """Ejecuta el flujo principal: navegación, filtros, exportación y verificación."""
        self.run_start = time.time()
        self._ensure_main_iframe()
        self.iframe_manager.switch_to_default_content()
        self._open_dynamic_checklist()
        self._open_sub_pm_query()
        self._prepare_filters()
        self._select_last_month()
        self._click_splitbutton("Filtrar")
        self._wait_for_list()
        self._click_splitbutton("Export sub WO detail")
        self._wait_for_loader()
        case_detected = monitor_export_loader(self.driver)
        require(case_detected, "No se detectó resultado de exportación antes del timeout")
        self._handle_export_result(case_detected)
        downloaded_file = self._verify_download()
        logger.info("🎉 Script completado exitosamente!")
        logger.info("📋 Navegación a Dynamic checklist > Sub PM Query completada")
        logger.info("🔧 Filtros aplicados y lista cargada")
        return downloaded_file

    def _ensure_main_iframe(self):
        """Asegura que el contexto inicial es el iframe de filtros."""
        require(
            self.iframe_manager.find_main_iframe(max_attempts=MAX_IFRAME_ATTEMPTS),
            "No se pudo localizar el iframe principal",
        )

    def _open_dynamic_checklist(self):
        """Abre el módulo Dynamic checklist desde el menú lateral."""
        logger.info("📋 Navegando a Dynamic checklist...")
        require(
            navigate_to_menu_item(self.driver, self.wait, 5, "Dynamic checklist", "Dynamic checklist"),
            "No se pudo navegar a Dynamic checklist",
        )

    def _open_sub_pm_query(self):
        """Selecciona la opción Sub PM Query dentro del módulo."""
        logger.info("🔍 Navegando a Sub PM Query...")
        require(
            navigate_to_submenu_item(
                self.wait, "//span[@class='level-1 link-nav' and @title='Sub PM Query']", "Sub PM Query"
            ),
            "No se pudo seleccionar Sub PM Query",
        )

    def _prepare_filters(self):
        """Cambia al iframe nuevo y abre el panel de filtros."""
        logger.info("⏳ Esperando a que cargue la sección Sub PM Query...")
        self._switch_to_last_iframe("Sub PM Query")
        self.filter_manager.wait_for_filters_ready()
        logger.info("✅ Sección Sub PM Query cargada correctamente")
        logger.info("🔧 Aplicando filtros...")
        require(self.filter_manager.open_filter_panel(method="simple"), "No se pudo abrir el panel de filtros")

    def _select_last_month(self):
        """Marca la opción de rango 'Último mes' dentro del panel de filtros."""
        logger.info("⏳ Esperando a que se cargue 'Último mes' en el panel...")
        radio_elements = self.wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".el-radio-button__inner"))
        )
        if len(radio_elements) < 8:
            raise RuntimeError(f"No se encontraron suficientes elementos radio. Encontrados: {len(radio_elements)}")
        radio_elements[7].click()
        logger.info("✓ 'Último mes' seleccionado en Complete time (8vo elemento)")
        sleep(1)

    def _click_splitbutton(self, label, pause=2):
        button = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, f"//span[@class='sdm_splitbutton_text' and contains(text(),'{label}')]"))
        )
        button.click()
        logger.info("✓ Botón '%s' presionado", label)
        if pause:
            sleep(pause)

    def _wait_for_list(self):
        """Confirma que la tabla principal esté disponible tras aplicar filtros."""
        logger.info("⏳ Esperando a que cargue la lista...")
        total_element = self.wait.until(
            EC.presence_of_element_located((By.XPATH, "//span[@class='el-pagination__total' and contains(text(),'Total')]"))
        )
        logger.info("✓ Lista cargada: %s", total_element.text)

    def _wait_for_loader(self):
        """Espera el loader que aparece cuando inicia la exportación."""
        logger.info("⏳ Esperando loader de exportación...")
        self.wait.until(
            EC.presence_of_element_located((By.XPATH, "//p[@class='el-loading-text' and contains(text(),'Exportando')]"))
        )
        logger.info("✓ Loader de exportación detectado: Exportando...")

    def _handle_export_result(self, case_detected):
        """Gestiona las dos variantes de exportación (directa o asincrónica)."""
        logger.info("🔍 Verificando resultado de exportación...")
        if case_detected == "log_management":
            logger.info("ℹ Mensaje de información detectado: Exportación en segundo plano")
            self._process_log_management()
        elif case_detected == "direct_download":
            logger.info("✓ Exportación directa completada - verificando descarga...")
        else:
            raise RuntimeError(f"Caso de exportación desconocido: {case_detected}")

    def _process_log_management(self):
        """Sigue el flujo de Log Management cuando la exportación corre en segundo plano."""
        self._close_export_prompt()
        self.iframe_manager.switch_to_default_content()
        logger.info("📋 Navegando a Log Management...")
        require(
            navigate_to_menu_item(self.driver, self.wait, 5, "Log Management", "Log Management"),
            "No se pudo navegar a Log Management",
        )
        require(
            navigate_to_submenu_item(self.wait, "//span[contains(text(),'Data Export Logs')]", "Data Export Logs"),
            "No se pudo abrir Data Export Logs",
        )
        logger.info("⏳ Cambiando al iframe de Data Export Logs...")
        self._switch_to_last_iframe("Data Export Logs")
        self._wait_for_list()
        self._monitor_log_management()

    def _close_export_prompt(self):
        """Intenta cerrar el modal de advertencia antes de cambiar de módulo."""
        try:
            close_button = self.wait.until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//button[@class='prompt-header-tool-btn keyboard-focus']//i[@class='prompt-header-close el-icon-close']",
                    )
                )
            )
            close_button.click()
            logger.info("✓ Mensaje de información cerrado")
            sleep(2)
        except Exception:
            logger.warning("⚠ No se pudo cerrar el mensaje", exc_info=True)

    def _monitor_log_management(self):
        """Revisa repetidamente el estado de la exportación y dispara la descarga final."""
        logger.info("🔍 Buscando exportación en progreso...")
        for attempt in range(1, MAX_STATUS_ATTEMPTS + 1):
            status = None
            try:
                target_row = self.driver.find_element(
                    By.XPATH, "//tr[contains(.,'[check_list_mobile/check_list_mobile/custom_excel]')]"
                )
                status = target_row.find_element(By.XPATH, ".//td[3]//span").text.strip()
                logger.info("📊 Status de exportación: %s", status)
                if status == "Succeed":
                    logger.info("✅ Exportación completada exitosamente!")
                    download_button = target_row.find_element(
                        By.XPATH,
                        ".//td[11]//div[contains(@class,'export-operation-text') and contains(text(),'Download')]",
                    )
                    download_button.click()
                    logger.info("✓ Click en 'Download' - archivo descargándose...")
                    sleep(3)
                    return
            except Exception:
                logger.exception("❌ Error al revisar exportación (intento %s)", attempt)

            if status == "Running":
                logger.info("⏳ Exportación en progreso... (intento %s/%s)", attempt, MAX_STATUS_ATTEMPTS)
            elif status and status != "Succeed":
                logger.warning("⚠ Status inesperado: %s", status)

            try:
                self._click_splitbutton("Refresh", pause=0)
            except Exception:
                logger.warning("⚠ Error al presionar Refresh", exc_info=True)
            sleep(30)

        message = "Tiempo máximo de espera alcanzado para la exportación"
        logger.error("⏱️ %s", message)
        raise RuntimeError(message)

    def _verify_download(self, timeout=120):
        """Busca un archivo nuevo en la carpeta de descargas dentro del tiempo límite."""
        logger.info("⏳ Verificando que el archivo se haya descargado...")
        deadline = time.time() + timeout
        while time.time() < deadline:
            candidates = [
                path
                for path in self.download_dir.iterdir()
                if path.is_file()
                and not path.name.endswith(".crdownload")
                and path.stat().st_mtime >= self.run_start
            ]
            if candidates:
                latest = max(candidates, key=lambda p: p.stat().st_mtime)
                logger.info("✓ Archivo descargado detectado: %s", latest.name)
                return latest
            sleep(2)

        message = f"No se encontró archivo descargado en {self.download_dir} dentro del tiempo esperado"
        logger.error(message)
        raise RuntimeError(message)

    def _switch_to_last_iframe(self, context_name):
        iframe_count = self.iframe_manager.get_iframe_count()
        require(iframe_count > 0, f"No se encontraron iframes en la sección {context_name}")
        require(
            self.iframe_manager.switch_to_iframe(iframe_count - 1),
            f"No se pudo cambiar al iframe de {context_name}",
        )


def run_dynamic_checklist(
    *,
    download_path=DOWNLOAD_PATH,
    headless=False,
    chrome_extra_args=None,
):
    """Punto de entrada reutilizable para ejecutar el flujo desde Airflow o scripts locales."""
    browser_manager = BrowserManager(
        download_path=download_path,
        headless=headless,
        extra_args=chrome_extra_args,
    )
    driver, wait = browser_manager.create_driver()
    downloaded_file = None
    try:
        auth_manager = AuthManager(driver)
        require(auth_manager.login(USERNAME, PASSWORD), "No se pudo realizar el login.")

        workflow = DynamicChecklistWorkflow(driver, wait, download_path=download_path)
        downloaded_file = workflow.run()
    except RuntimeError as exc:
        logger.error("❌ %s", exc)
        raise
    except Exception:
        logger.exception("❌ Error inesperado durante el proceso")
        raise
    finally:
        logger.info("ℹ Cerrando navegador...")
        browser_manager.close_driver()
    return downloaded_file


if __name__ == "__main__":
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO)

    run_dynamic_checklist()
