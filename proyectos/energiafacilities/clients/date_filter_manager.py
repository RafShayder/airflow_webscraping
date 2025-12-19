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
from typing import Any, List

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from common.dynamic_checklist_constants import (
    RADIO_BUTTON_LAST_MONTH_INDEX,
    CSS_RADIO_BUTTON,
    SLEEP_AFTER_RADIO_CLICK,
)

logger = logging.getLogger(__name__)

CREATETIME_FROM_CONTAINER_XPATH = (
    "//*[@id='nf1']/div[2]/div[2]/div[1]/div[2]/div[1]/div[1]"
)
CREATETIME_TO_CONTAINER_XPATH = (
    "//*[@id='nf1']/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]"
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
                # fecha_hasta = ayer (igual que scraper.py)
                hoy = datetime.now().date()
                date_to_date = hoy - timedelta(days=1)
                # fecha_desde = N días antes de fecha_hasta
                date_from_date = date_to_date - timedelta(days=last_n_days)
                date_from_str = date_from_date.strftime('%Y-%m-%d')
                date_to_str = date_to_date.strftime('%Y-%m-%d')
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
        Aplica fechas usando Create Time: replica exactamente la lógica de scraper.py.
        Establece fecha y hora para DESDE, luego fecha y hora para HASTA.
        
        Args:
            date_from_str: Fecha inicial en formato 'YYYY-MM-DD'
            date_to_str: Fecha final en formato 'YYYY-MM-DD'
        """
        try:
            # Horas por defecto (igual que scraper.py)
            hora_desde = "00:00:00"
            hora_hasta = "23:59:59"
            
            logger.debug("Aplicando fechas manuales: DESDE %s %s → HASTA %s %s", 
                         date_from_str, hora_desde, date_to_str, hora_hasta)

            # === DESDE ===
            logger.debug("Iniciando escritura de fecha DESDE: %s %s", date_from_str, hora_desde)
            self._click_y_setear_fecha_y_hora(
                CREATETIME_FROM_CONTAINER_XPATH, 
                date_from_str, 
                hora_desde
            )
            logger.debug("Fecha DESDE aplicada: %s %s", date_from_str, hora_desde)
            sleep(1.5)  # Esperar un poco más para que el popper se cierre completamente

            # Verificar que no haya poppers abiertos antes de continuar con HASTA
            for _ in range(10):
                open_pickers = self.driver.find_elements(
                    By.CSS_SELECTOR,
                    '.el-picker-panel.el-date-picker.has-time'
                )
                all_closed = True
                for p in open_pickers:
                    style = p.get_attribute('style') or ''
                    if 'display: none' not in style and p.is_displayed():
                        all_closed = False
                        break
                if all_closed:
                    break
                sleep(0.2)

            # === HASTA ===
            logger.debug("Iniciando escritura de fecha HASTA: %s %s", date_to_str, hora_hasta)
            self._click_y_setear_fecha_y_hora(
                CREATETIME_TO_CONTAINER_XPATH, 
                date_to_str, 
                hora_hasta
            )
            logger.debug("Fecha HASTA aplicada: %s %s", date_to_str, hora_hasta)
            sleep(1)  # Igual que scraper.py línea 581: time.sleep(1)

            logger.debug("Fechas aplicadas exitosamente: %s %s → %s %s", 
                       date_from_str, hora_desde, date_to_str, hora_hasta)
        except Exception as e:
            logger.warning("Error al aplicar fechas manuales: %s, usando fallback a 'Último mes'", e)
            self.select_last_month_radio()

    def _click_y_setear_fecha_y_hora(self, container_xpath: str, fecha: str, hora: str) -> None:
        """
        Replica exactamente la lógica de scraper.py para establecer fecha y hora en el selector.
        Primero establece la fecha, luego la hora, ambos con sus respectivos inputs.
        
        Args:
            container_xpath: XPATH del contenedor (DESDE o HASTA) - usado como fallback
            fecha: Fecha en formato string (YYYY-MM-DD)
            hora: Hora en formato string (HH:MM:SS)
            
        Raises:
            RuntimeError: Si no se encuentra el input de fecha o hora, o no se puede escribir
        """
        # 1) Buscar el row de Create time específicamente (no Complete time)
        create_time_row = None
        try:
            all_rows = self.driver.find_elements(By.CSS_SELECTOR, '.el-row.ows_filter_row, .ows_filter_row')
            for row in all_rows:
                row_text = row.text or ""
                if "Create time" in row_text or "Create Time" in row_text:
                    create_time_row = row
                    break
        except Exception:
            pass

        if create_time_row is None:
            raise RuntimeError("No se encontró el row de Create time.")

        # 2) Intenta abrir el modo "intervalo personalizado" con el botón + dentro de Create time
        try:
            # Buscar el botón + específicamente dentro del row de Create time
            plus = create_time_row.find_element(
                By.CSS_SELECTOR,
                ".ows_datetime_interval_customer_text.el-icon-circle-plus"
            )
            if plus.is_displayed():
                self._robust_click_element(plus)
                sleep(1)
        except Exception:
            # Si no existe el botón + (ya está abierto con el-icon-remove), continuar
            pass

        # 3) Buscar el contenedor de fechas personalizadas dentro de Create time y el input base
        input_base = None
        try:
            # Buscar el contenedor específicamente dentro del row de Create time
            customer_container = create_time_row.find_element(
                By.CSS_SELECTOR,
                ".ows_datetime_interval_customer"
            )
            # Buscar el input DESDE (primero) o HASTA (segundo) según el container_xpath
            # Verificar el ÚLTIMO div del XPath, no si contiene div[1] en cualquier parte
            inputs = customer_container.find_elements(By.CSS_SELECTOR, 'input.el-input__inner')
            # El XPath termina en div[1] para DESDE o div[2] para HASTA
            if container_xpath.endswith("div[1]"):
                # Es DESDE (primer input)
                input_base = inputs[0] if len(inputs) > 0 else None
                logger.debug("Identificado como DESDE (primer input)")
            elif container_xpath.endswith("div[2]"):
                # Es HASTA (segundo input)
                input_base = inputs[1] if len(inputs) > 1 else inputs[0] if len(inputs) > 0 else None
                logger.debug("Identificado como HASTA (segundo input)")
            else:
                # Fallback: usar el primer input
                input_base = inputs[0] if len(inputs) > 0 else None
                logger.warning(f"No se pudo identificar DESDE/HASTA del XPath: {container_xpath}")
        except Exception:
            # Fallback: usar XPath original
            try:
                cont = self.wait.until(
                    EC.element_to_be_clickable((By.XPATH, container_xpath))
                )
                input_base = cont.find_element(By.CSS_SELECTOR, 'input.el-input__inner')
            except Exception:
                pass

        if input_base is None:
            raise RuntimeError("No se encontró el input base para fecha/hora.")

        # 4) Cerrar cualquier popper abierto antes de abrir uno nuevo
        try:
            # Cerrar poppers abiertos presionando ESC o click fuera
            open_pickers = self.driver.find_elements(
                By.CSS_SELECTOR,
                '.el-picker-panel.el-date-picker.has-time'
            )
            for picker in open_pickers:
                style = picker.get_attribute('style') or ''
                if 'display: none' not in style and picker.is_displayed():
                    # Cerrar el popper presionando ESC
                    self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                    sleep(0.3)
                    break
        except Exception:
            pass

        # 5) Hacer click en el input base para abrir el popper
        self._robust_click_element(input_base)
        sleep(1)

        # 6) Esperar a que el popper sea visible (el que tiene has-time y no tiene display: none)
        picker = None
        for _ in range(30):  # Esperar hasta 3 segundos (30 * 0.1s)
            try:
                all_pickers = self.driver.find_elements(
                    By.CSS_SELECTOR,
                    '.el-picker-panel.el-date-picker.has-time'
                )
                for p in all_pickers:
                    style = p.get_attribute('style') or ''
                    if p.is_displayed() and 'display: none' not in style:
                        # Verificar que tenga los inputs que necesitamos
                        fecha_inputs = p.find_elements(
                            By.CSS_SELECTOR,
                            'input[placeholder="Seleccionar fecha"]'
                        )
                        if len(fecha_inputs) > 0:
                            picker = p
                            break
                if picker:
                    break
            except Exception:
                pass
            sleep(0.1)

        if picker is None:
            raise RuntimeError("El popper de fecha no se hizo visible después de hacer click.")

        # 7) Buscar input "Seleccionar fecha" dentro del popper visible
        target = None
        try:
            inputs_fecha = picker.find_elements(
                By.CSS_SELECTOR,
                'input.el-input__inner[placeholder="Seleccionar fecha"]'
            )
            for el in inputs_fecha:
                if el.is_displayed() and el.is_enabled():
                    target = el
                    break
        except Exception:
            pass

        if target is None:
            raise RuntimeError("No se encontró el input 'Seleccionar fecha' después de abrir el selector.")

        # Escribir la FECHA de forma robusta
        try:
            # Asegurarse de que el input esté enfocado
            target.click()
            sleep(0.5)
            # Limpiar el input
            target.send_keys(Keys.CONTROL, 'a')
            target.send_keys(Keys.DELETE)
            sleep(0.2)
            # Escribir la fecha
            target.send_keys(fecha)
            sleep(0.3)
            # Disparar eventos para que el framework detecte el cambio
            self.driver.execute_script(
                "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
                "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                target
            )
            sleep(0.3)
            # Presionar ENTER para confirmar la fecha
            target.send_keys(Keys.ENTER)
            sleep(0.5)
            
            # Verificar que la fecha se escribió correctamente
            fecha_escrita = target.get_attribute('value') or ''
            if fecha not in fecha_escrita and fecha_escrita:
                logger.warning(f"Fecha escrita no coincide. Esperado: {fecha}, Obtenido: {fecha_escrita}")
        except Exception as e:
            raise RuntimeError(f"No se pudo escribir la fecha '{fecha}': {e}")

        # 8) Buscar input "Seleccionar hora" dentro del mismo popper visible
        time_input = None
        # Esperar a que el input de hora esté disponible
        for _ in range(20):  # Esperar hasta 2 segundos
            try:
                inputs_hora = picker.find_elements(
                    By.CSS_SELECTOR,
                    'input.el-input__inner[placeholder="Seleccionar hora"]'
                )
                for el in inputs_hora:
                    if el.is_displayed() and el.is_enabled():
                        time_input = el
                        break
                if time_input:
                    break
            except Exception:
                pass
            sleep(0.1)

        if time_input is None:
            raise RuntimeError("No se encontró el input 'Seleccionar hora' o no está visible.")

        try:
            # Hacer scroll y click en el input de hora (igual que scraper.py línea 550)
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", time_input
            )
            sleep(0.2)
            time_input.click()
            sleep(0.5)  # Igual que scraper.py línea 551
            
            # Limpiar el input de forma más agresiva (la hora actual puede estar pre-llenada)
            # Intentar borrar múltiples veces para asegurar que se borre la hora por defecto
            for _ in range(3):
                time_input.send_keys(Keys.CONTROL, 'a')
                time_input.send_keys(Keys.DELETE)
                # También intentar con COMMAND en Mac
                try:
                    time_input.send_keys(Keys.COMMAND, 'a')
                    time_input.send_keys(Keys.DELETE)
                except Exception:
                    pass
                sleep(0.1)
            
            # Verificar que esté vacío antes de escribir (usar JavaScript si es necesario)
            valor_antes = time_input.get_attribute('value') or ''
            if valor_antes:
                # Si aún tiene valor, usar JavaScript para limpiarlo directamente
                self.driver.execute_script("arguments[0].value = '';", time_input)
                sleep(0.2)
            
            # Escribir la hora completa HH:MM:SS (igual que scraper.py línea 554)
            # scraper.py escribe la hora completa, no solo HH:MM
            time_input.send_keys(hora)
            sleep(0.3)
            
            # Disparar eventos (igual que scraper.py línea 555-559)
            self.driver.execute_script(
                "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
                "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                time_input
            )
            sleep(0.3)
            
            # Presionar ENTER (igual que scraper.py línea 560)
            time_input.send_keys(Keys.ENTER)
            sleep(1)  # Igual que scraper.py línea 561: time.sleep(1)
            
            # Verificar que la hora se escribió
            hora_escrita = time_input.get_attribute('value') or ''
            logger.debug(f"Hora escrita: {hora_escrita}")
            
            # Si la hora no se aplicó correctamente, intentar confirmar con el botón del time panel
            if hora not in hora_escrita and hora_escrita:
                try:
                    # Buscar el botón "Confirmar" del time panel si está visible
                    time_panel = picker.find_element(By.CSS_SELECTOR, '.el-time-panel')
                    if time_panel and time_panel.is_displayed():
                        confirm_btn = time_panel.find_element(
                            By.CSS_SELECTOR,
                            'button.el-time-panel__btn.confirm'
                        )
                        if confirm_btn and confirm_btn.is_displayed():
                            confirm_btn.click()
                            sleep(0.5)
                except Exception:
                    pass
        except Exception as e:
            raise RuntimeError(f"No se pudo escribir la hora '{hora}': {e}")

        # 9) Cerrar el popper de forma más agresiva
        try:
            # Intentar encontrar y presionar el botón "Confirmar"
            confirm_buttons = picker.find_elements(
                By.CSS_SELECTOR,
                'button.el-picker-panel__link-btn'
            )
            for btn in confirm_buttons:
                btn_text = btn.text or ""
                if btn.is_displayed() and ('Confirmar' in btn_text or 'Confirm' in btn_text):
                    btn.click()
                    sleep(0.5)
                    break
        except Exception:
            pass
        
        # Asegurarse de que el popper se cierre (múltiples intentos)
        for _ in range(3):
            try:
                # Verificar si el popper sigue abierto
                open_pickers = self.driver.find_elements(
                    By.CSS_SELECTOR,
                    '.el-picker-panel.el-date-picker.has-time'
                )
                still_open = False
                for p in open_pickers:
                    style = p.get_attribute('style') or ''
                    if 'display: none' not in style and p.is_displayed():
                        still_open = True
                        break
                
                if still_open:
                    # Presionar ESC
                    self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                    sleep(0.3)
                else:
                    break
            except Exception:
                break
        
        # Esperar un poco más para asegurar que el popper se cerró completamente
        sleep(0.5)

    def _robust_click_element(self, element: WebElement) -> bool:
        """
        Intenta click con varias estrategias (igual que robust_click de scraper.py).
        """
        try:
            element.click()
            return True
        except Exception:
            try:
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", element
                )
                sleep(0.2)
                element.click()
                return True
            except Exception:
                try:
                    self.driver.execute_script("arguments[0].click();", element)
                    return True
                except Exception:
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
                "No se encontró radio etiquetado como 'Último mes'. "
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
