"""
Helpers comunes para acciones con Selenium.

Resumen:
- ``require``: valida condiciones críticas en los workflows.
- ``wait_for_notification_to_clear``: espera a que desaparezcan banners flotantes
  (por ejemplo ``.el-notification`` en el portal Integratel).
- ``click_with_retry``: intenta un click y, si es interceptado, recurre a JavaScript
  después de limpiar notificaciones.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Iterable, Optional, Sequence

from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)


def require(condition: bool, message: str) -> None:
    """Simplifica las aserciones dentro de los workflows."""
    if not condition:
        raise RuntimeError(message)


def wait_for_notification_to_clear(
    driver: WebDriver,
    timeout: int = 8,
    poll_frequency: float = 0.5,
) -> None:
    """
    Espera a que desaparezcan las notificaciones flotantes de ElementUI (``.el-notification``).
    """
    try:
        WebDriverWait(driver, timeout, poll_frequency=poll_frequency).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, ".el-notification"))
        )
    except TimeoutException:
        # No es crítico: seguimos adelante, pero dejamos un warning para diagnóstico.
        logger.debug("Notificación sigue visible después de %ss", timeout)


def click_with_retry(
    element: WebElement,
    driver: WebDriver,
    *,
    description: str = "",
    attempts: int = 3,
    wait_between: float = 1.0,
) -> None:
    """
    Ejecuta ``element.click()`` con reintentos. Si el click es interceptado por un
    overlay/botón invisible, intenta limpiar notificaciones y hace click con JS.
    """
    description = description or repr(element)
    last_error: Optional[Exception] = None

    for attempt in range(1, attempts + 1):
        try:
            element.click()
            return
        except ElementClickInterceptedException as exc:
            logger.debug(
                "Click interceptado en %s (intento %s/%s): %s",
                description,
                attempt,
                attempts,
                exc,
            )
            last_error = exc
            wait_for_notification_to_clear(driver, timeout=3)
            time.sleep(wait_between)
            try:
                driver.execute_script("arguments[0].click();", element)
                logger.debug("Click vía JavaScript para %s completado.", description)
                return
            except Exception as js_exc:
                last_error = js_exc
                logger.debug("Click JS también falló: %s", js_exc)
                time.sleep(wait_between)
        except Exception as exc:
            last_error = exc
            logger.debug(
                "Error haciendo click en %s (intento %s/%s): %s",
                description,
                attempt,
                attempts,
                exc,
            )
            time.sleep(wait_between)

    if last_error:
        raise last_error


def navigate_to_menu_item(
    driver: WebDriver,
    wait: WebDriverWait,
    menu_index: int,
    item_title: str,
    item_name: str,
    *,
    logger: logging.Logger = logger,
    hover_pause: float = 1.0,
    click_pause: float = 2.0,
) -> None:
    """
    Realiza hover y click sobre un elemento del menú lateral (índice basado en cero).
    """
    menu_items = wait.until(lambda d: d.find_elements(By.CSS_SELECTOR, ".menu-item.sideItem"))
    logger.info("ℹ Encontrados %s elementos del menú", len(menu_items))
    require(len(menu_items) > menu_index, f"No se encontró el menú {item_name}")

    target_menu_item = menu_items[menu_index]
    ActionChains(driver).move_to_element(target_menu_item).perform()
    logger.info("✓ Hover realizado sobre el elemento del menú (índice %s)", menu_index)
    time.sleep(hover_pause)

    wait.until(EC.element_to_be_clickable((By.XPATH, f"//span[@title='{item_title}']"))).click()
    logger.info("✓ %s seleccionado", item_name)
    time.sleep(click_pause)


def navigate_to_submenu(
    wait: WebDriverWait,
    submenu_xpath: str,
    submenu_name: str,
    *,
    logger: logging.Logger = logger,
    click_pause: float = 3.0,
) -> None:
    """Selecciona un submenú dentro del panel lateral."""
    wait.until(EC.element_to_be_clickable((By.XPATH, submenu_xpath))).click()
    logger.info("✓ %s seleccionado", submenu_name)
    time.sleep(click_pause)


def monitor_export_loader(
    driver: WebDriver,
    *,
    timeout: int = 300,
    logger: logging.Logger = logger,
) -> str:
    """
    Observa el comportamiento del loader de exportación y devuelve:
    - ``'log_management'`` si aparece el mensaje de 60 segundos.
    - ``'direct_download'`` si el loader desaparece sin mensaje adicional.
    """
    logger.info("⏳ Esperando a que termine la exportación...")
    logger.info(
        "ℹ Nota: La exportación puede tardar hasta %s segundos, por favor espere...",
        timeout,
    )
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            info_message = driver.find_elements(
                By.XPATH,
                "//div[@class='prompt-window']//span[contains(text(),'Se ha tardado 60 segundos') "
                "or contains(text(),'60 seconds')]",
            )
            if info_message:
                logger.info("✓ Caso detectado: la exportación continuará en Log Management")
                return "log_management"

            loader_present = driver.find_elements(
                By.XPATH,
                "//p[@class='el-loading-text' and (contains(text(),'Exportando') or contains(text(),'Exporting'))]",
            )

            if not loader_present:
                logger.info("ℹ Loader desapareció - esperando posible mensaje de aviso...")
                additional_start = time.time()
                while time.time() - additional_start < 10:
                    info_message = driver.find_elements(
                        By.XPATH,
                        "//div[@class='prompt-window']//span[contains(text(),'Se ha tardado 60 segundos') "
                        "or contains(text(),'60 seconds')]",
                    )
                    if info_message:
                        logger.info("✓ Aviso tardío detectado - ir a Log Management")
                        return "log_management"
                    time.sleep(1)

                logger.info("✓ Loader finalizado sin aviso - descarga directa")
                return "direct_download"

            time.sleep(2)
        except Exception:
            logger.exception("⚠ Error monitoreando exportación")
            time.sleep(2)

    logger.warning("⏱️ Timeout monitoreando exportación (considerar caso log_management)")
    return "log_management"


def wait_for_download(
    download_dir: Path,
    *,
    since: float,
    overwrite: bool,
    timeout: int,
    desired_name: Optional[str],
    logger: logging.Logger = logger,
    initial_snapshot: Optional[Sequence[Path]] = None,
) -> Path:
    """
    Espera a que aparezca un archivo nuevo en ``download_dir``.

    ``since`` representa el timestamp a partir del cual considerar archivos válidos.
    """
    logger.info("⏳ Verificando que el archivo se haya descargado...")
    deadline = time.time() + timeout
    before = set(initial_snapshot or [])

    while time.time() < deadline:
        current = [p for p in download_dir.iterdir() if p.is_file()]

        if before:
            candidates = [
                p
                for p in current
                if p not in before and not p.name.endswith(".crdownload") and p.stat().st_mtime >= since
            ]
        else:
            candidates = [
                p
                for p in current
                if not p.name.endswith(".crdownload") and p.stat().st_mtime >= since
            ]

        if candidates:
            latest = max(candidates, key=lambda p: p.stat().st_mtime)
            logger.info("✓ Archivo descargado detectado: %s", latest.name)
            if desired_name:
                target_path = _resolve_target_filename(download_dir, desired_name, overwrite)
                try:
                    if overwrite and target_path.exists():
                        target_path.unlink()
                    latest.rename(target_path)
                    latest = target_path
                    logger.info("📦 Archivo renombrado a: %s", target_path.name)
                except Exception as exc:
                    message = f"No se pudo renombrar el archivo descargado a {target_path.name}"
                    logger.error("❌ %s", message, exc_info=True)
                    raise RuntimeError(message) from exc
            return latest

        time.sleep(2)

    raise RuntimeError(f"No se encontró archivo descargado en {download_dir} dentro del tiempo esperado")


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
