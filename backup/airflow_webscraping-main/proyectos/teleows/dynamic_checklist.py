import logging
import os
import time
from pathlib import Path
from time import sleep

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from .config import (
    DOWNLOAD_PATH,
    MAX_IFRAME_ATTEMPTS,
    MAX_STATUS_ATTEMPTS,
    PASSWORD,
    USERNAME,
    DYNAMIC_CHECKLIST_OUTPUT_FILENAME,
    EXPORT_OVERWRITE_FILES,
)
from .core.auth_manager import AuthManager
from .core.browser_manager import BrowserManager
from .core.filter_manager import FilterManager
from .core.iframe_manager import IframeManager


# Flujo automatizado para navegar a Dynamic checklist, aplicar filtros y descargar el reporte.
# Todas las operaciones están estructuradas en helpers y métodos privados dentro del workflow.
logger = logging.getLogger(__name__)


def require(condition, message):
    """Utilidad para detener el flujo cuando una condición obligatoria no se cumple."""
    if not condition:
        raise RuntimeError(message)


def navigate_to_menu_item(driver, wait, menu_index, item_title, item_name):
    try:
        # Localizamos todos los items del menú lateral; algunos submenús requieren hover previo.
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

        # Tras el hover, el ítem se vuelve clickeable mediante su título visible.
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
        # El submenú se muestra después de expandir la opción padre; esperamos hasta poder hacer click.
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

    # Existen dos comportamientos: descarga directa o exportación en segundo plano (Log Management).
    while time.time() - start_time < 300:
        try:
            # 1. Buscar mensaje de aviso (puede aparecer mientras el loader está visible)
            # Intentar en ambos idiomas
            info_message = driver.find_elements(
                By.XPATH, "//div[@class='prompt-window']//span[contains(text(),'Se ha tardado 60 segundos') or contains(text(),'60 seconds')]"
            )
            if info_message:
                case_detected = "log_management"
                logger.info("✓ Caso 2 detectado: Mensaje de aviso apareció - navegar a Log Management")
                break

            # 2. Verificar si el loader desapareció (ambos idiomas)
            loader_present = driver.find_elements(
                By.XPATH, "//p[@class='el-loading-text' and (contains(text(),'Exportando') or contains(text(),'Exporting'))]"
            )
            
            if not loader_present:
                logger.info("ℹ Loader desapareció - esperando mensaje de aviso (puede aparecer en 2-3 segundos)...")
                
                # 3. Esperar un poco más por el mensaje de aviso
                # (El mensaje puede aparecer DESPUÉS de que el loader desaparece)
                additional_start = time.time()
                while time.time() - additional_start < 10:
                    info_message = driver.find_elements(
                        By.XPATH, "//div[@class='prompt-window']//span[contains(text(),'Se ha tardado 60 segundos') or contains(text(),'60 seconds')]"
                    )
                    if info_message:
                        case_detected = "log_management"
                        logger.info("✓ Caso 2 detectado: Mensaje de aviso apareció después del loader - navegar a Log Management")
                        break
                    sleep(1)

                # 4. Si no apareció el mensaje, es descarga directa
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
    def __init__(
        self,
        driver,
        wait,
        download_path=DOWNLOAD_PATH,
        status_timeout=None,
        status_poll_interval=30,
        output_filename=None,
    ):
        self.driver = driver
        self.wait = wait
        # Gestores utilitarios reutilizados en el flujo para manejar iframes, filtros y navegador.
        self.iframe_manager = IframeManager(driver)
        self.filter_manager = FilterManager(driver, wait)
        self.download_dir = Path(download_path).resolve()
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.run_start = time.time()
        # Mantiene compatibilidad con MAX_STATUS_ATTEMPTS (30 segundos por intento era el valor anterior).
        default_timeout = MAX_STATUS_ATTEMPTS * 30
        self.status_timeout = status_timeout if status_timeout is not None else default_timeout
        self.status_poll_interval = status_poll_interval
        self.desired_filename = (output_filename or "").strip() or None
        self._overwrite_files = EXPORT_OVERWRITE_FILES

    def _resolve_target_filename(self, desired_name):
        target = self.download_dir / Path(desired_name).name
        if self._overwrite_files or not target.exists():
            return target
        stem = target.stem
        suffix = target.suffix
        counter = 1
        while True:
            candidate = target.with_name(f"{stem}_{counter}{suffix}")
            if not candidate.exists():
                return candidate
            counter += 1

    def run(self):
        """Ejecuta el flujo principal: navegación, filtros, exportación y verificación."""
        self.run_start = time.time()
        # 1) Preparación de contexto: asegurar iframe y navegar hasta Sub PM Query.
        self._ensure_main_iframe()
        self.iframe_manager.switch_to_default_content()
        self._open_dynamic_checklist()
        self._open_sub_pm_query()
        # 2) Configuración de filtros sobre el iframe recién cargado.
        self._prepare_filters()
        self._select_last_month()
        self._click_splitbutton("Filter")  # Botón en inglés
        self._wait_for_list()
        # 3) Lanzamos la exportación y esperamos que la plataforma indique su estado.
        self._click_splitbutton("Export sub WO detail")
        self._wait_for_loader()
        case_detected = monitor_export_loader(self.driver)
        require(case_detected, "No se detectó resultado de exportación antes del timeout")
        # 4) Dependiendo del caso, completamos el flujo de exportación.
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
        # La sección abre un iframe adicional; tomamos el último disponible.
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
        # El octavo elemento corresponde al rango 'Último mes' en la sección Complete time.
        radio_elements[7].click()
        logger.info("✓ 'Último mes' seleccionado en Complete time (8vo elemento)")
        sleep(1)

    def _click_splitbutton(self, label, pause=2):
        # Los splitbuttons comparten clase; filtramos con el texto visible para reutilizar el helper.
        try:
            # Intentar primero con el selector específico
            button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, f"//span[@class='sdm_splitbutton_text' and contains(text(),'{label}')]"))
            )
            button.click()
            logger.info("✓ Botón '%s' presionado", label)
        except Exception as e:
            logger.warning(f"⚠ No se encontró el botón con selector específico, intentando alternativas...")
            
            # Tomar screenshot para debug
            try:
                import time
                screenshot_path = f"/app/temp/error_button_{label}_{int(time.time())}.png"
                self.driver.save_screenshot(screenshot_path)
                logger.info(f"📸 Screenshot guardado en: {screenshot_path}")
            except:
                pass
            
            # Intentar selector alternativo (cualquier elemento con el texto)
            try:
                # Listar todos los botones disponibles para debug
                try:
                    all_buttons = self.driver.execute_script('''
                        const buttons = Array.from(document.querySelectorAll('button, .sdm_splitbutton_text, [role="button"]'));
                        return buttons.map(b => b.textContent?.trim() || '').filter(t => t);
                    ''')
                    logger.info(f"🔍 Botones encontrados en la página: {all_buttons}")
                except:
                    pass
                
                button = self.wait.until(
                    EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(),'{label}')]"))
                )
                button.click()
                logger.info("✓ Botón '%s' presionado (selector alternativo)", label)
            except Exception as e2:
                logger.error(f"❌ No se pudo encontrar el botón '{label}' con ningún selector: {e2}")
                raise
        
        if pause:
            sleep(pause)

    def _wait_for_list(self):
        """Confirma que la tabla principal esté disponible tras aplicar filtros."""
        logger.info("⏳ Esperando a que cargue la lista...")
        # Aprovechamos el texto "Total" de la paginación para validar que la tabla ya está renderizada.
        total_element = self.wait.until(
            EC.presence_of_element_located((By.XPATH, "//span[@class='el-pagination__total' and contains(text(),'Total')]"))
        )
        logger.info("✓ Lista cargada: %s", total_element.text)

    def _wait_for_loader(self):
        """Espera el loader que aparece cuando inicia la exportación."""
        logger.info("⏳ Esperando loader de exportación...")
        # Antes de monitorear casos, confirmamos que la plataforma haya lanzado el proceso.
        # Intentar ambos idiomas: español e inglés
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.common.exceptions import TimeoutException
        
        try:
            # Intentar en español
            self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//p[@class='el-loading-text' and contains(text(),'Exportando')]"))
            )
            logger.info("✓ Loader de exportación detectado: Exportando...")
        except TimeoutException:
            try:
                # Intentar en inglés
                self.wait.until(
                    EC.presence_of_element_located((By.XPATH, "//p[@class='el-loading-text' and contains(text(),'Exporting')]"))
                )
                logger.info("✓ Loader de exportación detectado: Exporting...")
            except TimeoutException:
                # Intentar cualquier loader genérico
                self.wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "el-loading-text"))
                )
                logger.info("✓ Loader de exportación detectado (genérico)")

    def _handle_export_result(self, case_detected):
        """Gestiona las dos variantes de exportación (directa o asincrónica)."""
        logger.info("🔍 Verificando resultado de exportación...")
        # Según el caso detectado, decidimos permanecer en la página o ir a Log Management.
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
        # Una vez en la tabla de logs, monitorizamos el estado hasta poder descargar.
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
            # El cierre no es crítico, pero dejamos registro si falla.
            logger.warning("⚠ No se pudo cerrar el mensaje", exc_info=True)

    def _monitor_log_management(self):
        """Revisa repetidamente el estado de la exportación y dispara la descarga final."""
        logger.info("🔍 Buscando exportación en progreso...")
        end_states = {"Succeed", "Failed", "Aborted", "Waiting", "Concurrent Waiting"}
        deadline = time.time() + self.status_timeout
        attempt = 0

        # Consultamos la tabla hasta obtener un estado terminal o agotar el timeout configurado.
        while time.time() < deadline:
            attempt += 1
            try:
                try:
                    self._click_splitbutton("Refresh", pause=0)
                except Exception:
                    logger.warning("⚠ Error al presionar Refresh", exc_info=True)
                sleep(2)

                # Filtramos directamente la fila del job relacionado con Dynamic checklist.
                target_row = self.driver.find_element(
                    By.XPATH, "//tr[contains(.,'[check_list_mobile/check_list_mobile/custom_excel]')]"
                )
                status = target_row.find_element(By.XPATH, ".//td[3]//span").text.strip()
                logger.info(
                    "📊 Status de exportación: %s (intento %s, quedan %.0f s)",
                    status,
                    attempt,
                    max(0, deadline - time.time()),
                )

                if status in end_states:
                    if status == "Succeed":
                        logger.info("✅ Exportación completada exitosamente!")
                        # El enlace de descarga aparece en la misma fila; hacemos click para iniciar el archivo.
                        download_button = target_row.find_element(
                            By.XPATH,
                            ".//td[11]//div[contains(@class,'export-operation-text') and contains(text(),'Download')]",
                        )
                        download_button.click()
                        logger.info("✓ Click en 'Download' - archivo descargándose...")
                        sleep(3)
                        return
                    raise RuntimeError(f"Proceso de exportación terminó con estado: {status}")

                if status != "Running":
                    logger.warning("⚠ Status inesperado: %s", status)

            except RuntimeError:
                raise
            except Exception:
                logger.exception("❌ Error al revisar exportación (intento %s)", attempt)

            sleep(self.status_poll_interval)

        message = "Tiempo máximo de espera alcanzado para la exportación"
        logger.error("⏱️ %s", message)
        raise RuntimeError(message)

    def _verify_download(self, timeout=120):
        """Busca un archivo nuevo en la carpeta de descargas dentro del tiempo límite."""
        logger.info("⏳ Verificando que el archivo se haya descargado...")
        deadline = time.time() + timeout
        while time.time() < deadline:
            # Listamos archivos recientes ignorando descargas en progreso (.crdownload).
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
                if self.desired_filename:
                    target_path = self._resolve_target_filename(self.desired_filename)
                    try:
                        if self._overwrite_files and target_path.exists():
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

        message = f"No se encontró archivo descargado en {self.download_dir} dentro del tiempo esperado"
        logger.error(message)
        raise RuntimeError(message)

    def _switch_to_last_iframe(self, context_name):
        iframe_count = self.iframe_manager.get_iframe_count()
        require(iframe_count > 0, f"No se encontraron iframes en la sección {context_name}")
        # Muchas vistas abren iframes incrementales; asumimos que el último es el más reciente.
        require(
            self.iframe_manager.switch_to_iframe(iframe_count - 1),
            f"No se pudo cambiar al iframe de {context_name}",
        )


def run_dynamic_checklist(
    *,
    download_path=DOWNLOAD_PATH,
    headless=False,
    chrome_extra_args=None,
    status_timeout=None,
    status_poll_interval=30,
    output_filename=None,
):
    """Punto de entrada reutilizable para ejecutar el flujo desde Airflow o scripts locales."""
    # El gestor de navegador controla la configuración de Chrome (descargas, headless, parámetros extra).
    browser_manager = BrowserManager(
        download_path=download_path,
        headless=headless,
        extra_args=chrome_extra_args,
    )
    driver, wait = browser_manager.create_driver()
    downloaded_file = None
    try:
        # 1) Autenticamos con las credenciales configuradas para acceder al módulo.
        auth_manager = AuthManager(driver)
        require(auth_manager.login(USERNAME, PASSWORD), "No se pudo realizar el login.")

        # 2) Construimos el workflow con parámetros personalizados (descargas, timeouts, etc.).
        workflow = DynamicChecklistWorkflow(
            driver,
            wait,
            download_path=download_path,
            status_timeout=status_timeout,
            status_poll_interval=status_poll_interval,
            output_filename=output_filename if output_filename is not None else DYNAMIC_CHECKLIST_OUTPUT_FILENAME,
        )
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
