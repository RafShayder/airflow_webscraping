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
from typing import Optional, Any

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from ..common.vue_helpers import inject_date_via_javascript
from ..common.dynamic_checklist_constants import (
    RADIO_BUTTON_LAST_MONTH_INDEX,
    MIN_RADIO_BUTTONS_REQUIRED,
    CSS_RADIO_BUTTON,
    SLEEP_AFTER_RADIO_CLICK,
)

logger = logging.getLogger(__name__)


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
                logger.info("📅 Aplicando últimos %s días: %s → %s",
                           last_n_days, date_from_str, date_to_str)
            else:
                date_from_str = getattr(settings, 'date_from', None)
                date_to_str = getattr(settings, 'date_to', None)
                if not date_from_str or not date_to_str:
                    logger.warning("⚠️ date_mode=1 pero no hay last_n_days ni date_from/date_to, usando fallback")
                    self.select_last_month_radio()
                    return
                logger.info("📅 Aplicando fechas manuales: %s → %s", date_from_str, date_to_str)

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
        logger.info("🔍 Buscando grupo de radio Create Time y botón '+'...")
        
        # Buscar grupo de radio de Create Time
        radio_groups = self.driver.find_elements(By.CSS_SELECTOR, ".el-radio-group")
        create_time_row = None
        
        for group in radio_groups:
            try:
                parent_row = group.find_element(By.XPATH, "./ancestor::div[contains(@class, 'el-row')]")
                row_text = parent_row.text.strip().lower()
                row_html = parent_row.get_attribute("outerHTML").lower()
                
                if ("create" in row_text or "create" in row_html) and ("time" in row_text or "time" in row_html):
                    logger.info("✓ Encontrado grupo de radio de Create Time")
                    create_time_row = parent_row
                    break
            except:
                continue
        
        if not create_time_row:
            logger.warning("⚠️ No se encontró fila de Create Time, usando fallback a radio button")
            self.select_last_month_radio()
            return
        
        # Buscar botón "+" para abrir los inputs de fecha
        plus_button = None
        customer_container = create_time_row.find_elements(By.CSS_SELECTOR, 
            ".ows_datetime_interval_customer_container, .ows_datetime_interval_customer_text")
        
        if customer_container:
            for cont in customer_container:
                try:
                    plus_icon = cont.find_element(By.CSS_SELECTOR, 
                        ".el-icon-circle-plus, span.el-icon-circle-plus, i.el-icon-circle-plus")
                    if plus_icon.is_displayed() and plus_icon.is_enabled():
                        plus_button = plus_icon
                        break
                except:
                    pass
        
        if not plus_button:
            try:
                plus_button = create_time_row.find_element(By.CSS_SELECTOR, 
                    "span.el-icon-circle-plus, .el-icon-circle-plus")
            except:
                pass
        
        if not plus_button:
            logger.warning("⚠️ No se encontró botón '+', usando fallback a radio button")
            self.select_last_month_radio()
            return
        
        # Hacer clic en el botón "+" para abrir los inputs
        logger.info("🖱️ Haciendo clic en el botón '+' para abrir selector de fechas...")
        plus_button.click()
        sleep(0.5)  # Esperar a que se abran los inputs
        
        # Buscar los inputs DESDE/HASTA que aparecieron
        date_inputs_info = self.driver.execute_script("""
            const allInputs = Array.from(document.querySelectorAll('input[type="text"]'));
            const results = [];
            
            allInputs.forEach((inp, idx) => {
                let vueComponent = null;
                let parent = inp.parentElement;
                while (parent && !vueComponent) {
                    if (parent.__vue__) {
                        vueComponent = parent.__vue__;
                        break;
                    }
                    parent = parent.parentElement;
                }
                
                let isDateEditor = false;
                parent = inp.parentElement;
                while (parent && parent !== document.body) {
                    const classes = parent.className || '';
                    if (classes.includes('date') || classes.includes('Date') || 
                        classes.includes('time') || classes.includes('Time') ||
                        classes.includes('picker') || classes.includes('Picker') ||
                        classes.includes('el-date-editor')) {
                        isDateEditor = true;
                        break;
                    }
                    parent = parent.parentElement;
                }
                
                const placeholder = inp.placeholder || '';
                const isDatePlaceholder = placeholder && (
                    placeholder.toLowerCase().includes('start') ||
                    placeholder.toLowerCase().includes('end') ||
                    placeholder.toLowerCase().includes('desde') ||
                    placeholder.toLowerCase().includes('hasta') ||
                    placeholder.toLowerCase().includes('date') ||
                    placeholder.toLowerCase().includes('fecha')
                );
                
                if ((vueComponent || isDateEditor || isDatePlaceholder) && inp.offsetParent !== null) {
                    results.push(idx);
                }
            });
            
            return results;
        """)
        
        all_selenium_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='text']")
        date_inputs = [all_selenium_inputs[idx] for idx in date_inputs_info if idx < len(all_selenium_inputs)]
        
        if len(date_inputs) < 2:
            logger.warning("⚠️ No se encontraron suficientes inputs de fecha después de hacer clic en '+', usando fallback")
            self.select_last_month_radio()
            return
        
        input_from = date_inputs[0]
        input_to = date_inputs[1]
        
        # Aplicar fechas usando el método que funciona
        logger.info("📅 Aplicando fecha DESDE: %s", date_from_str)
        self.apply_date_to_input(input_from, f"{date_from_str} 00:00:00", date_from_str, "DESDE")
        
        logger.info("📅 Aplicando fecha HASTA: %s", date_to_str)
        self.apply_date_to_input(input_to, f"{date_to_str} 23:59:59", date_to_str, "HASTA")
        
        logger.info("✅ Fechas aplicadas exitosamente")

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
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", input_element)
                wait = WebDriverWait(self.driver, 5)
                wait.until(EC.element_to_be_clickable(input_element))
                try:
                    input_element.click()
                except:
                    self.driver.execute_script("arguments[0].click();", input_element)
            except:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", input_element)
                self.driver.execute_script("arguments[0].click();", input_element)
            
            sleep(0.3)  # Esperar a que se abra el date picker
            
            # Aplicar fecha usando JavaScript con formato completo
            date_parts = date_display.split("-")
            if len(date_parts) == 3:
                target_year = int(date_parts[0])
                target_month = int(date_parts[1])
                target_day = int(date_parts[2])
                date_with_time = f"{target_year}-{target_month:02d}-{target_day:02d} 00:00:00" if field_name == "DESDE" else f"{target_year}-{target_month:02d}-{target_day:02d} 23:59:59"
            else:
                date_with_time = date_value
            
            # Usar helper de vue_helpers
            inject_date_via_javascript(self.driver, input_element, date_with_time, date_display)
            
            sleep(0.2)  # Esperar a que se procese
            
            # Verificar si el date picker está abierto después de Enter
            picker_visible = False
            try:
                picker_panels = self.driver.find_elements(By.CSS_SELECTOR, ".el-picker-panel, .el-date-picker")
                for panel in picker_panels:
                    if panel.is_displayed():
                        picker_visible = True
                        break
            except:
                pass
            
            # Si el picker se cerró después de Enter, verificar si la fecha ya está confirmada
            confirm_clicked = False
            if not picker_visible:
                input_value_after_enter = input_element.get_attribute("value")
                if len(date_parts) == 3:
                    expected_date_str = f"{target_year}-{target_month:02d}-{target_day:02d}"
                    expected_date_with_time = f"{expected_date_str} 00:00:00" if field_name == "DESDE" else f"{expected_date_str} 23:59:59"
                else:
                    expected_date_str = date_display
                    expected_date_with_time = date_display
                
                date_still_correct = (date_display in input_value_after_enter or 
                                    date_display.replace("-", "/") in input_value_after_enter or
                                    expected_date_str in input_value_after_enter or
                                    expected_date_with_time in input_value_after_enter)
                
                if date_still_correct:
                    confirm_clicked = True
                else:
                    # Volver a abrir el picker para confirmar
                    try:
                        input_element.click()
                        sleep(0.3)
                        picker_visible = True
                    except:
                        pass
            
            # Solo buscar el botón Confirmar si el picker está abierto Y no se confirmó ya
            if picker_visible and not confirm_clicked:
                confirm_selectors = [
                    ".el-picker-panel__footer button.el-picker-panel__link-btn",
                    ".el-picker-panel__footer button",
                ]
                
                for selector in confirm_selectors:
                    try:
                        confirm_buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for btn in confirm_buttons:
                            btn_text = btn.text.strip()
                            if btn.is_displayed() and btn.is_enabled() and btn_text == "Confirmar":
                                input_value_before = input_element.get_attribute("value")
                                if len(date_parts) == 3:
                                    expected_date_str = f"{target_year}-{target_month:02d}-{target_day:02d}"
                                    expected_date_with_time = f"{expected_date_str} 00:00:00" if field_name == "DESDE" else f"{expected_date_str} 23:59:59"
                                else:
                                    expected_date_str = date_display
                                    expected_date_with_time = date_display
                                
                                date_correct = (date_display in input_value_before or 
                                              date_display.replace("-", "/") in input_value_before or
                                              expected_date_str in input_value_before or
                                              expected_date_with_time in input_value_before)
                                
                                if date_correct:
                                    btn.click()
                                    sleep(0.5)
                                    confirm_clicked = True
                                    break
                        if confirm_clicked:
                            break
                    except:
                        continue
            
            sleep(0.3)  # Esperar para que se procese
            
            # Verificar valor final
            final_value = input_element.get_attribute("value")
            if len(date_parts) == 3:
                expected_date_str = f"{target_year}-{target_month:02d}-{target_day:02d}"
                expected_date_with_time = f"{expected_date_str} 00:00:00" if field_name == "DESDE" else f"{expected_date_str} 23:59:59"
            else:
                expected_date_str = date_display
                expected_date_with_time = date_display
            
            if (date_display in final_value or 
                date_display.replace("-", "/") in final_value or
                expected_date_str in final_value or
                expected_date_with_time in final_value):
                return True
            
            logger.warning("⚠️ La fecha no se aplicó correctamente para %s. Esperado: '%s', Obtenido: '%s'", 
                          field_name, date_display, final_value)
            return False
            
        except Exception as e:
            logger.error("❌ Error al aplicar fecha %s: %s", field_name, e)
            return False

    def select_last_month_radio(self) -> None:
        """Marca la opción de rango 'Último mes' dentro del panel de filtros (radio button)."""
        logger.info("📅 Seleccionando rango rápido: Último mes")
        logger.info("⏳ Esperando a que se cargue 'Último mes' en el panel...")
        radio_elements = self.wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, CSS_RADIO_BUTTON))
        )
        if len(radio_elements) < MIN_RADIO_BUTTONS_REQUIRED:
            raise RuntimeError(
                f"No se encontraron suficientes elementos radio. Encontrados: {len(radio_elements)}"
            )
        # El octavo elemento corresponde al rango 'Último mes' en la sección Complete time.
        radio_elements[RADIO_BUTTON_LAST_MONTH_INDEX].click()
        logger.info("✓ 'Último mes' seleccionado en Complete time (8vo elemento)")
        sleep(SLEEP_AFTER_RADIO_CLICK)

