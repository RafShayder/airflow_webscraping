"""
DateFilterManager: maneja la aplicación de filtros de fecha en workflows de scraping.

Proporciona métodos para aplicar filtros de fecha usando diferentes estrategias:
- Radio buttons para rangos predefinidos (último mes)
- Inputs de fecha manuales usando Create Time (botón +, inputs DESDE/HASTA)
"""

from __future__ import annotations

import logging
from time import sleep
from datetime import datetime, timedelta
from typing import Optional, Any, List, Tuple

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException

from energiafacilities.common.vue_helpers import inject_date_via_javascript
from energiafacilities.common.dynamic_checklist_constants import (
    RADIO_BUTTON_LAST_MONTH_INDEX,
    CSS_RADIO_BUTTON,
    SLEEP_AFTER_RADIO_CLICK,
)

logger = logging.getLogger(__name__)

CREATETIME_ROW_ID = "createtimeRow"
CREATETIME_FROM_CONTAINER_XPATH = (
    "//*[@id='createtimeRow']/div[2]/div[2]/div/div[2]/div/div[1]"
)
CREATETIME_TO_CONTAINER_XPATH = (
    "//*[@id='createtimeRow']/div[2]/div[2]/div/div[2]/div/div[2]"
)

class DateFilterManager:
    """Maneja la aplicación de filtros de fecha en paneles de filtrado."""

    def __init__(self, driver: WebDriver, wait: WebDriverWait):
        self.driver = driver
        self.wait = wait

    def apply_date_filters(self, settings: Any) -> None:
        """
        Configura filtros de fecha según settings.date_mode.

        - date_mode=1 → aplica fechas manuales usando Create Time (botón +, inputs DESDE/HASTA)
        - date_mode=2 o default → selecciona el radio button "Último mes"
        
        Args:
            settings: Objeto con atributos date_mode, last_n_days, date_from, date_to
        """
        date_mode = getattr(settings, 'date_mode', 2)
        
        if date_mode == 1:
            # Calcular fechas dinámicamente si last_n_days está configurado
            last_n_days = getattr(settings, 'last_n_days', None)
            if last_n_days is not None:
                date_to = datetime.now()
                date_from = date_to - timedelta(days=last_n_days)
                date_from_str = date_from.strftime('%Y-%m-%d')
                date_to_str = date_to.strftime('%Y-%m-%d')
                logger.debug("Aplicando últimos %s días: %s → %s",
                           last_n_days, date_from_str, date_to_str)
            else:
                date_from_str = getattr(settings, 'date_from', None)
                date_to_str = getattr(settings, 'date_to', None)
                if not date_from_str or not date_to_str:
                    logger.warning("date_mode=1 pero no hay last_n_days ni date_from/date_to, usando fallback")
                    self.select_last_month_radio()
                    return
                logger.debug("Aplicando fechas manuales: %s → %s", date_from_str, date_to_str)

            # Aplicar fechas usando Create Time (botón + y inputs DESDE/HASTA)
            self.apply_create_time_dates(date_from_str, date_to_str)
        else:
            # date_mode=2 o default: usar radio button "Último mes"
            self.select_last_month_radio()

    def apply_create_time_dates(self, date_from_str: str, date_to_str: str) -> None:
        """
        Aplica fechas usando Create Time: busca el botón +, abre los inputs y aplica las fechas.
        
        Args:
            date_from_str: Fecha inicial en formato 'YYYY-MM-DD'
            date_to_str: Fecha final en formato 'YYYY-MM-DD'
        """
        logger.debug("Buscando grupo de radio Create Time y botón '+'...")

        if self._apply_dates_via_createtime_row(date_from_str, date_to_str):
            logger.debug("Fechas aplicadas mediante contenedores de Create Time")
            return
        logger.debug("No se pudo usar contenedores directos, intentando método alternativo")
        create_time_row = self._find_createtime_row()
        if not create_time_row:
            logger.debug("No se encontró fila de Create Time, usando fallback a radio button")
            self.select_last_month_radio()
            return

        if not self._click_createtime_plus_button(create_time_row):
            logger.debug("No se encontró botón '+', usando fallback a radio button")
            self.select_last_month_radio()
            return

        try:
            date_inputs = self._wait_for_manual_date_inputs(create_time_row)
        except TimeoutException:
            logger.debug("No aparecieron inputs DESDE/HASTA después de abrir Create Time, usando fallback")
            self.select_last_month_radio()
            return

        if len(date_inputs) < 2:
            logger.debug("No se encontraron suficientes inputs de fecha después de hacer clic en '+', usando fallback")
            self.select_last_month_radio()
            return

        input_from, input_to = date_inputs[:2]

        logger.debug("Aplicando fecha DESDE: %s", date_from_str)
        from_ok = self.apply_date_to_input(input_from, f"{date_from_str} 00:00:00", date_from_str, "DESDE")

        logger.debug("Aplicando fecha HASTA: %s", date_to_str)
        to_ok = self.apply_date_to_input(input_to, f"{date_to_str} 23:59:59", date_to_str, "HASTA")

        if not (from_ok and to_ok):
            logger.warning("Al menos una fecha no se aplicó correctamente, usando fallback a 'Último mes'")
            self.select_last_month_radio()
            return

        logger.info("Fechas aplicadas exitosamente")

    def apply_date_to_input(
        self, 
        input_element: WebElement, 
        date_value: str, 
        date_display: str, 
        field_name: str
    ) -> bool:
        """
        Aplica una fecha a un input usando JavaScript y Vue.
        
        Args:
            input_element: Elemento input del date picker
            date_value: Valor completo con hora (ej: '2025-09-27 00:00:00')
            date_display: Valor de fecha sin hora (ej: '2025-09-27')
            field_name: Nombre del campo ('DESDE' o 'HASTA')
            
        Returns:
            True si la fecha se aplicó correctamente, False en caso contrario
        """
        try:
            # Hacer scroll y abrir date picker
            try:
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", input_element
                )
                wait = WebDriverWait(self.driver, 5)
                wait.until(EC.element_to_be_clickable(input_element))
                try:
                    input_element.click()
                except Exception:
                    self.driver.execute_script("arguments[0].click();", input_element)
            except Exception:
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", input_element
                )
                self.driver.execute_script("arguments[0].click();", input_element)

            sleep(0.3)  # Esperar a que se abra el date picker

            expected_date_str, expected_date_with_time = self._build_expected_date_values(
                date_display, field_name
            )

            # Usar helper de vue_helpers
            inject_date_via_javascript(
                self.driver, input_element, date_value, date_display
            )

            sleep(0.2)  # Esperar a que se procese

            picker_visible = self._is_picker_visible()
            confirm_clicked = False

            if not picker_visible:
                input_value_after_enter = input_element.get_attribute("value") or ""
                if self._value_matches_expected(
                    input_value_after_enter,
                    date_display,
                    expected_date_str,
                    expected_date_with_time,
                ):
                    confirm_clicked = True
                else:
                    # Volver a abrir el picker para confirmar
                    try:
                        input_element.click()
                        sleep(0.3)
                        picker_visible = self._is_picker_visible()
                    except Exception:
                        pass

            if picker_visible and not confirm_clicked:
                confirm_clicked = self._confirm_picker_value(
                    input_element,
                    date_display,
                    expected_date_str,
                    expected_date_with_time,
                )

            sleep(0.3)  # Esperar para que se procese

            final_value = input_element.get_attribute("value") or ""
            if self._value_matches_expected(
                final_value, date_display, expected_date_str, expected_date_with_time
            ):
                return True

            logger.warning(
                "⚠️ La fecha no se aplicó correctamente para %s. Esperado: '%s', Obtenido: '%s'",
                field_name,
                date_display,
                final_value,
            )
            return False

        except Exception as error:
            logger.error("Error al aplicar fecha %s: %s", field_name, error)
            return False

    def select_last_month_radio(self) -> None:
        """Marca la opción de rango 'Último mes' dentro del panel de filtros."""
        logger.debug("Seleccionando rango rápido: Último mes")
        preferred_labels = ("último mes", "ultimo mes", "last month")

        candidate_selectors = [
            CSS_RADIO_BUTTON,
            ".el-radio__label",
            ".el-radio-button__inner",
        ]

        radio_elements: List[WebElement] = []
        for selector in candidate_selectors:
            try:
                radio_elements.extend(self.driver.find_elements(By.CSS_SELECTOR, selector))
            except Exception:
                continue

        if not radio_elements:
            raise RuntimeError("No se encontraron radios para seleccionar 'Último mes'")

        target_element = None
        for element in radio_elements:
            text = element.text.strip().lower()
            if any(label in text for label in preferred_labels if text):
                target_element = element
                break

        if target_element is None:
            logger.warning(
                "⚠️ No se encontró radio etiquetado como 'Último mes'. "
                "Intentando usar índice por defecto..."
            )
            default_elements = self.driver.find_elements(By.CSS_SELECTOR, CSS_RADIO_BUTTON)
            if len(default_elements) > RADIO_BUTTON_LAST_MONTH_INDEX:
                target_element = default_elements[RADIO_BUTTON_LAST_MONTH_INDEX]
            else:
                target_element = radio_elements[-1]

        self._click_radio_element(target_element)
        logger.debug("Rango 'Último mes' seleccionado")
        sleep(SLEEP_AFTER_RADIO_CLICK)

    def _click_radio_element(self, element: WebElement) -> None:
        """Intenta hacer click en el radio recibido aplicando scroll/JS si es necesario."""
        try:
            element.click()
            return
        except Exception:
            pass

        try:
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", element
            )
            sleep(0.1)
            element.click()
            return
        except Exception:
            pass

        self.driver.execute_script("arguments[0].click();", element)

    def _find_createtime_row(self) -> Optional[WebElement]:
        """Localiza la fila que contiene los radios de Create Time."""
        radio_groups = self.driver.find_elements(By.CSS_SELECTOR, ".el-radio-group")
        for group in radio_groups:
            try:
                parent_row = group.find_element(
                    By.XPATH, "./ancestor::div[contains(@class, 'el-row')]"
                )
            except Exception:
                continue

            row_text = (parent_row.text or "").strip().lower()
            row_html = (parent_row.get_attribute("outerHTML") or "").lower()
            if ("create" in row_text or "create" in row_html) and (
                "time" in row_text or "time" in row_html
            ):
                logger.debug("Encontrado grupo de radio de Create Time")
                return parent_row
        return None

    def _wait_for_manual_date_inputs(
        self, container: Optional[WebElement], timeout: int = 5
    ) -> List[WebElement]:
        """Espera a que aparezcan los inputs personalizados DESDE/HASTA."""
        if container is None:
            return []

        wait = WebDriverWait(self.driver, timeout)
        wait.until(lambda _: len(self._locate_manual_date_inputs(container)) >= 2)
        return self._locate_manual_date_inputs(container)

    def _locate_manual_date_inputs(self, container: Optional[WebElement]) -> List[WebElement]:
        """Devuelve los inputs de texto asociados al row Create Time."""
        if container is None:
            return []

        try:
            inputs = container.find_elements(
                By.CSS_SELECTOR, "input.el-input__inner[placeholder]"
            )
        except Exception:
            return []

        placeholders = ("seleccionar fecha", "seleccionar", "select date", "fecha", "date")
        visible_inputs: List[WebElement] = []
        for element in inputs:
            try:
                if not element.is_displayed() or not element.is_enabled():
                    continue
                placeholder = (element.get_attribute("placeholder") or "").strip().lower()
                if any(word in placeholder for word in placeholders):
                    visible_inputs.append(element)
            except Exception:
                continue

        return visible_inputs

    def _apply_dates_via_createtime_row(
        self, date_from: str, date_to: str
    ) -> bool:
        """Intenta aplicar fechas usando los contenedores fijos del row Create Time."""
        try:
            row = self.driver.find_element(By.ID, CREATETIME_ROW_ID)
        except Exception:
            return False

        if not self._click_createtime_plus_button(row):
            logger.debug("No se pudo abrir el modo personalizado con el botón '+' fijo")
            return False

        from_ok = self._set_date_in_createtime_container(
            CREATETIME_FROM_CONTAINER_XPATH, date_from, "DESDE", row
        )
        to_ok = self._set_date_in_createtime_container(
            CREATETIME_TO_CONTAINER_XPATH, date_to, "HASTA", row
        )
        self._ensure_picker_closed()
        return from_ok and to_ok

    def _click_createtime_plus_button(self, context: Optional[WebElement]) -> bool:
        """Intenta abrir el selector personalizado haciendo clic en el botón '+'."""
        selectors = [
            ".ows_datetime_interval_customer_text.el-icon-circle-plus",
            ".ows_datetime_interval_customer_container .el-icon-circle-plus",
            ".el-icon-circle-plus",
        ]

        scopes: List[Any] = []
        if context is not None:
            scopes.append(context)
        scopes.append(self.driver)

        for scope in scopes:
            for selector in selectors:
                try:
                    button = scope.find_element(By.CSS_SELECTOR, selector)  # type: ignore[attr-defined]
                except AttributeError:
                    button = self.driver.find_element(By.CSS_SELECTOR, selector)
                except Exception:
                    continue

                try:
                    displayed = button.is_displayed()
                    enabled = button.is_enabled()
                except Exception:
                    continue

                if displayed and enabled:
                    try:
                        button.click()
                    except Exception:
                        self.driver.execute_script("arguments[0].click();", button)
                    sleep(0.2)
                    return True
        return False

    def _set_date_in_createtime_container(
        self,
        container_xpath: str,
        date_value: str,
        label: str,
        context_row: Optional[WebElement] = None,
    ) -> bool:
        """Selecciona el contenedor indicado y aplica la fecha provista."""
        if context_row is not None:
            self._click_createtime_plus_button(context_row)

        try:
            container = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, container_xpath))
            )
        except Exception:
            logger.debug("No se encontró contenedor %s para Create Time", label)
            return False

        self._robust_click(container)
        sleep(0.2)

        input_element = self._find_visible_date_input(container)
        if not input_element:
            logger.debug("No se encontró input visible para %s", label)
            return False

        if not self._fill_date_input(input_element, date_value):
            logger.debug("No se pudo escribir la fecha %s en %s", date_value, label)
            return False

        logger.debug("%s aplicado: %s", label, date_value)
        return True

    def _find_visible_date_input(
        self, container: Optional[WebElement]
    ) -> Optional[WebElement]:
        """Devuelve el input visible de fecha, priorizando el contenedor activo."""
        placeholders = ("seleccionar fecha", "seleccionar", "select date", "select", "fecha", "date")

        if container is not None:
            try:
                inner_inputs = container.find_elements(
                    By.CSS_SELECTOR, "input.el-input__inner[placeholder]"
                )
                for element in inner_inputs:
                    try:
                        if element.is_displayed() and element.is_enabled():
                            return element
                    except Exception:
                        continue
            except Exception:
                pass

        inputs = self.driver.find_elements(
            By.CSS_SELECTOR, "input.el-input__inner[placeholder]"
        )
        for element in inputs:
            try:
                if not element.is_displayed() or not element.is_enabled():
                    continue
            except Exception:
                continue
            placeholder = (element.get_attribute("placeholder") or "").lower()
            if any(word in placeholder for word in placeholders):
                return element

        return None

    def _fill_date_input(self, input_element: WebElement, date_value: str) -> bool:
        """Escribe la fecha en el input usando eventos compatibles con Element UI."""
        try:
            input_element.click()
            sleep(0.1)
            input_element.send_keys(Keys.CONTROL, "a")
            input_element.send_keys(Keys.DELETE)
            try:
                input_element.send_keys(Keys.COMMAND, "a")
                input_element.send_keys(Keys.DELETE)
            except Exception:
                pass
            input_element.send_keys(date_value)
            self.driver.execute_script(
                "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
                "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                input_element,
            )
            input_element.send_keys(Keys.ENTER)
            sleep(0.2)
            return True
        except Exception as exc:
            logger.error("Error al llenar input de fecha: %s", exc)
            return False

    def _robust_click(self, element: WebElement) -> bool:
        """Replica el click resiliente del script original."""
        try:
            element.click()
            return True
        except Exception:
            pass

        try:
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", element
            )
            sleep(0.2)
            element.click()
            return True
        except Exception:
            pass

        try:
            self.driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False

    def _ensure_picker_closed(self) -> None:
        """Cierra paneles de fecha abiertos para liberar el botón de filtros."""
        for _ in range(3):
            try:
                panels = self.driver.find_elements(
                    By.CSS_SELECTOR, ".el-picker-panel, .el-date-picker"
                )
                visible = [panel for panel in panels if panel.is_displayed()]
                if not visible:
                    return
                body = self.driver.find_element(By.TAG_NAME, "body")
                body.send_keys(Keys.ESCAPE)
                sleep(0.2)
            except Exception:
                try:
                    self.driver.execute_script("document.body.click();")
                    sleep(0.2)
                except Exception:
                    break

    def _build_expected_date_values(self, date_display: str, field_name: str) -> Tuple[str, str]:
        """Genera pares esperados (fecha simple, fecha con hora) para validar inputs."""
        date_parts = date_display.split("-")
        if len(date_parts) == 3:
            target_year = int(date_parts[0].strip())
            target_month = int(date_parts[1].strip())
            day_part = date_parts[2].strip().split()[0]
            target_day = int(day_part)
            expected_date_str = f"{target_year}-{target_month:02d}-{target_day:02d}"
            expected_date_with_time = (
                f"{expected_date_str} 00:00:00"
                if field_name == "DESDE"
                else f"{expected_date_str} 23:59:59"
            )
        else:
            expected_date_str = date_display
            expected_date_with_time = date_display

        return expected_date_str, expected_date_with_time

    def _value_matches_expected(
        self,
        value: str,
        date_display: str,
        expected_date_str: str,
        expected_date_with_time: str,
    ) -> bool:
        """Verifica si el valor del input coincide con alguna de las combinaciones esperadas."""
        normalized_value = value or ""
        alt_display = date_display.replace("-", "/")
        patterns = [
            date_display,
            alt_display if alt_display != date_display else "",
            expected_date_str,
            expected_date_with_time,
        ]
        return any(pattern and pattern in normalized_value for pattern in patterns)

    def _confirm_picker_value(
        self,
        input_element: WebElement,
        date_display: str,
        expected_date_str: str,
        expected_date_with_time: str,
    ) -> bool:
        """Busca y presiona el botón Confirmar cuando el picker está visible."""
        confirm_selectors = [
            ".el-picker-panel__footer button.el-picker-panel__link-btn",
            ".el-picker-panel__footer button",
        ]

        input_value_before = input_element.get_attribute("value") or ""
        if not self._value_matches_expected(
            input_value_before, date_display, expected_date_str, expected_date_with_time
        ):
            return False

        for selector in confirm_selectors:
            try:
                confirm_buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
            except Exception:
                continue

            for btn in confirm_buttons:
                try:
                    btn_text = btn.text.strip()
                except Exception:
                    continue
                if btn.is_displayed() and btn.is_enabled() and btn_text == "Confirmar":
                    try:
                        btn.click()
                    except Exception:
                        self.driver.execute_script("arguments[0].click();", btn)
                    sleep(0.3)
                    return True
        return False

    def _is_picker_visible(self) -> bool:
        """Indica si alguno de los paneles de fecha está visible actualmente."""
        try:
            picker_panels = self.driver.find_elements(
                By.CSS_SELECTOR, ".el-picker-panel, .el-date-picker"
            )
        except Exception:
            return False

        for panel in picker_panels:
            try:
                if panel.is_displayed():
                    return True
            except Exception:
                continue
        return False
