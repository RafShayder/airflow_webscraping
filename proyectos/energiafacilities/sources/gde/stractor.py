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

from ...clients import AuthManager, BrowserManager, FilterManager, IframeManager
from ...common import require, wait_for_download
from ...teleows_config import TeleowsSettings
from ...core.utils import load_settings

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
            logger.info("📅 Aplicando últimos %s días: %s → %s", settings.last_n_days, date_from_str, date_to_str)
        else:
            date_from_str = settings.date_from
            date_to_str = settings.date_to
            logger.info("📅 Aplicando fechas manuales: %s → %s", date_from_str, date_to_str)

        # Los inputs de Element UI Date Editor para CREATE TIME (no complete time)
        # Usar createtimeRow en lugar de closetimeRow
        # CRÍTICO: Primero hacer clic en el botón "+" para abrir los campos de fecha personalizados
        wait = WebDriverWait(driver, 10)
        try:
            # Esperar a que createtimeRow esté disponible
            wait.until(EC.presence_of_element_located((By.ID, "createtimeRow")))
            createtime_row = driver.find_element(By.ID, "createtimeRow")
            
            # Buscar y hacer clic en el botón "+" para abrir los campos de fecha personalizados
            logger.info("🔘 Buscando botón '+' para abrir campos de fecha personalizados...")
            plus_button = None
            
            # Estrategia 1: Buscar botón con icono "+" o "plus" dentro de createtimeRow
            try:
                # Buscar botones con icono plus o círculo azul con +
                plus_buttons = createtime_row.find_elements(By.CSS_SELECTOR, "button, .el-button, [class*='plus'], [class*='add'], [class*='icon-plus']")
                for btn in plus_buttons:
                    try:
                        # Verificar si el botón contiene un "+" o icono plus
                        btn_text = btn.text.strip()
                        btn_html = btn.get_attribute('innerHTML') or ''
                        # Buscar icono plus o texto "+"
                        if '+' in btn_text or '+' in btn_html or 'plus' in btn_html.lower() or 'el-icon-plus' in btn_html:
                            # Verificar si está visible
                            is_visible = driver.execute_script("""
                                var elem = arguments[0];
                                if (!elem) return false;
                                var style = window.getComputedStyle(elem);
                                var rect = elem.getBoundingClientRect();
                                return style.display !== 'none' && 
                                       style.visibility !== 'hidden' && 
                                       style.opacity !== '0' &&
                                       rect.width > 0 && 
                                       rect.height > 0;
                            """, btn)
                            if is_visible:
                                plus_button = btn
                                logger.info("   ✅ Botón '+' encontrado")
                                break
                    except Exception:
                        continue
            except Exception as e:
                logger.debug(f"   ⚠️ Estrategia 1 para botón '+' falló: {e}")
            
            # Estrategia 2: Buscar con JavaScript directamente
            if not plus_button:
                try:
                    plus_button = driver.execute_script("""
                        const createtimeRow = document.querySelector('#createtimeRow');
                        if (!createtimeRow) return null;
                        
                        // Buscar botones dentro de createtimeRow
                        const buttons = createtimeRow.querySelectorAll('button, .el-button, [class*="plus"], [class*="add"]');
                        for (let btn of buttons) {
                            const text = (btn.textContent || btn.innerText || '').trim();
                            const html = btn.innerHTML || '';
                            const style = window.getComputedStyle(btn);
                            const rect = btn.getBoundingClientRect();
                            
                            // Verificar si es el botón plus (contiene + o icono plus)
                            if ((text.includes('+') || html.includes('+') || html.includes('plus') || html.includes('el-icon-plus')) &&
                                style.display !== 'none' && 
                                style.visibility !== 'hidden' && 
                                style.opacity !== '0' &&
                                rect.width > 0 && 
                                rect.height > 0) {
                                return btn;
                            }
                        }
                        return null;
                    """)
                    if plus_button:
                        logger.info("   ✅ Botón '+' encontrado con JavaScript")
                except Exception as e:
                    logger.debug(f"   ⚠️ Estrategia 2 para botón '+' falló: {e}")
            
            # Hacer clic en el botón "+" si se encontró
            if plus_button:
                try:
                    logger.info("   📌 Haciendo clic en botón '+' para abrir campos de fecha...")
                    driver.execute_script("arguments[0].click();", plus_button)
                    sleep(2.0)  # Esperar a que se abran los campos
                    logger.info("   ✅ Campos de fecha personalizados abiertos")
                except Exception as e:
                    logger.warning(f"   ⚠️ No se pudo hacer clic en botón '+': {e}")
                    # Intentar con clic normal
                    try:
                        plus_button.click()
                        sleep(2.0)
                        logger.info("   ✅ Campos de fecha personalizados abiertos (método alternativo)")
                    except Exception as e2:
                        logger.warning(f"   ⚠️ Método alternativo también falló: {e2}")
            else:
                logger.warning("   ⚠️ No se encontró el botón '+', intentando continuar...")
                # Esperar un poco por si los campos ya están abiertos
                sleep(1.0)
            
            # Ahora buscar los inputs de fecha personalizados (Del/al)
            inputs = createtime_row.find_elements(By.CSS_SELECTOR, ".el-date-editor input[type='text']")
            if len(inputs) < 2:
                # Fallback: buscar cualquier input
                inputs = createtime_row.find_elements(By.CSS_SELECTOR, "input[type='text']")
            
            # Si aún no hay suficientes inputs, esperar un poco más
            if len(inputs) < 2:
                logger.warning("   ⚠️ Campos de fecha aún no visibles, esperando...")
                sleep(2.0)
                inputs = createtime_row.find_elements(By.CSS_SELECTOR, ".el-date-editor input[type='text'], input[type='text']")
            
            if len(inputs) < 2:
                raise RuntimeError(f"No se encontraron suficientes inputs en createtimeRow después de abrir campos (encontrados: {len(inputs)})")
            
            input_from_elem = inputs[0]  # Primer input = desde (Del)
            input_to_elem = inputs[1]    # Segundo input = hasta (al)
            
            logger.info("✓ Campos de fecha CREATE TIME encontrados y listos")
        except Exception as e:
            logger.warning("⚠️ No se encontraron los elementos de fecha CREATE TIME: %s", e)
            sleep(2)
            raise RuntimeError("No se pudieron encontrar los campos de fecha CREATE TIME") from e

        # Agregar hora a las fechas (formato datetime requerido por Element UI)
        date_from_datetime = f"{date_from_str} 00:00:00"
        date_to_datetime = f"{date_to_str} 23:59:59"
        
        # Función helper para aplicar fecha a un componente Vue usando el elemento directamente
        def apply_date_to_vue_component(input_element, date_value, date_display, field_name):
            """Aplica una fecha al componente Vue y al input usando el elemento Selenium."""
            script = """
                const input = arguments[0];
                const dateValue = arguments[1];
                const dateValueDisplay = arguments[2];
                
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
                
                // Actualizar componente Vue (Vue 2) - MÚLTIPLES ESTRATEGIAS
                if (vueComponent) {
                    // Estrategia 1: Usar $set para actualizar reactivamente
                    if (vueComponent.$set) {
                        vueComponent.$set(vueComponent, 'value', dateValue);
                        vueComponent.$set(vueComponent, 'displayValue', dateValueDisplay);
                        // También actualizar directamente las propiedades
                        vueComponent.value = dateValue;
                        vueComponent.displayValue = dateValueDisplay;
                    } else {
                        // Si no hay $set, actualizar directamente
                        vueComponent.value = dateValue;
                        vueComponent.displayValue = dateValueDisplay;
                    }
                    
                    // Forzar actualización del componente
                    if (vueComponent.$forceUpdate) vueComponent.$forceUpdate();
                    
                    // Disparar eventos Vue
                    if (vueComponent.$emit) {
                        vueComponent.$emit('input', dateValue);
                        vueComponent.$emit('change', dateValue);
                        vueComponent.$emit('blur', dateValue);
                    }
                    
                    // También actualizar el modelo del componente padre si existe
                    if (vueComponent.$parent && vueComponent.$parent.$set) {
                        // Buscar el nombre del campo en el modelo del padre
                        const fieldName = input.getAttribute('name') || input.id || '';
                        if (fieldName) {
                            vueComponent.$parent.$set(vueComponent.$parent, fieldName, dateValue);
                        }
                    }
                }
                
                // Actualizar input usando setter nativo (bypass de React/Vue)
                const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                nativeInputValueSetter.call(input, dateValueDisplay);
                
                // Disparar eventos nativos del DOM (más completo)
                const events = ['focus', 'input', 'change', 'blur'];
                events.forEach(eventType => {
                    const event = new Event(eventType, { bubbles: true, cancelable: true });
                    input.dispatchEvent(event);
                });
                
                // También disparar eventos con datos
                const inputEvent = new Event('input', { bubbles: true, cancelable: true });
                Object.defineProperty(inputEvent, 'target', { value: input, enumerable: true });
                input.dispatchEvent(inputEvent);
                
                const changeEvent = new Event('change', { bubbles: true, cancelable: true });
                Object.defineProperty(changeEvent, 'target', { value: input, enumerable: true });
                input.dispatchEvent(changeEvent);
                
                // Sincronizar con el formulario padre si existe
                let form = input.closest('form');
                if (!form) {
                    form = input.closest('[class*="filter"], [class*="panel"]');
                }
                if (form) {
                    // Disparar evento de cambio en el formulario
                    const formChangeEvent = new Event('change', { bubbles: true });
                    form.dispatchEvent(formChangeEvent);
                }
                
                return true;
            """
            result = driver.execute_script(script, input_element, date_value, date_display)
            if not result:
                raise RuntimeError(f"No se pudo aplicar la fecha {field_name}")
            logger.info(f"✓ Fecha {field_name} aplicada: {date_display}")
            sleep(0.5)
        
        # Función helper para hacer clic en el botón "Confirmar" del panel de fecha
        def click_confirm_date_picker():
            """Hace clic en el botón 'Confirmar' del panel de fecha abierto con múltiples estrategias."""
            try:
                # Estrategia 1: Buscar botón Confirmar con múltiples selectores
                selectors = [
                    ".el-picker-panel__footer button.el-picker-panel__link-btn",
                    ".el-picker-panel__footer .el-picker-panel__link-btn",
                    "button.el-picker-panel__link-btn",
                    ".el-date-picker__header-btn--confirm",
                ]
                
                for selector in selectors:
                    try:
                        confirm_buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                        for btn in confirm_buttons:
                            try:
                                # Verificar con JavaScript si está visible y habilitado
                                is_visible = driver.execute_script("""
                                    var elem = arguments[0];
                                    if (!elem) return false;
                                    var style = window.getComputedStyle(elem);
                                    var rect = elem.getBoundingClientRect();
                                    return style.display !== 'none' && 
                                           style.visibility !== 'hidden' && 
                                           style.opacity !== '0' &&
                                           rect.width > 0 && 
                                           rect.height > 0 &&
                                           !elem.disabled;
                                """, btn)
                                
                                btn_text = driver.execute_script("return arguments[0].textContent || arguments[0].innerText || '';", btn).strip()
                                
                                if is_visible and ("Confirmar" in btn_text or "Confirm" in btn_text):
                                    logger.debug("   ✓ Haciendo clic en 'Confirmar' (JavaScript click)...")
                                    # Usar JavaScript click para mayor compatibilidad
                                    driver.execute_script("arguments[0].click();", btn)
                                    sleep(1.0)
                                    return True
                            except Exception:
                                continue
                    except Exception:
                        continue
                
                # Estrategia 2: Buscar por XPath con texto
                xpath_selectors = [
                    "//button[contains(@class, 'el-picker-panel__link-btn') and (contains(text(), 'Confirmar') or contains(text(), 'Confirm'))]",
                    "//button[contains(text(), 'Confirmar')]",
                    "//button[contains(text(), 'Confirm')]",
                ]
                
                for xpath in xpath_selectors:
                    try:
                        confirm_btn = driver.find_element(By.XPATH, xpath)
                        is_visible = driver.execute_script("""
                            var elem = arguments[0];
                            if (!elem) return false;
                            var style = window.getComputedStyle(elem);
                            var rect = elem.getBoundingClientRect();
                            return style.display !== 'none' && 
                                   style.visibility !== 'hidden' && 
                                   style.opacity !== '0' &&
                                   rect.width > 0 && 
                                   rect.height > 0 &&
                                   !elem.disabled;
                        """, confirm_btn)
                        
                        if is_visible:
                            logger.debug("   ✓ Haciendo clic en 'Confirmar' (XPath, JavaScript click)...")
                            driver.execute_script("arguments[0].click();", confirm_btn)
                            sleep(1.0)
                            return True
                    except Exception:
                        continue
                
                # Estrategia 3: Buscar directamente en el DOM con JavaScript
                try:
                    clicked = driver.execute_script("""
                        // Buscar todos los botones en el footer del panel
                        const panels = document.querySelectorAll('.el-picker-panel');
                        for (let panel of panels) {
                            if (panel.style.display !== 'none') {
                                const buttons = panel.querySelectorAll('button');
                                for (let btn of buttons) {
                                    const text = (btn.textContent || btn.innerText || '').trim();
                                    if (text.includes('Confirmar') || text.includes('Confirm')) {
                                        const style = window.getComputedStyle(btn);
                                        if (style.display !== 'none' && style.visibility !== 'hidden') {
                                            btn.click();
                                            return true;
                                        }
                                    }
                                }
                            }
                        }
                        return false;
                    """)
                    if clicked:
                        logger.debug("   ✓ Haciendo clic en 'Confirmar' (JavaScript directo DOM)...")
                        sleep(1.0)
                        return True
                except Exception:
                    pass
                
                logger.debug("   ⚠️ No se encontró botón 'Confirmar' visible después de múltiples intentos")
                return False
            except Exception as e:
                logger.debug(f"   ⚠️ Error al buscar botón Confirmar: {e}")
                return False
        
        # Función helper para cerrar el panel de fecha de forma robusta
        def close_date_picker_panel():
            """Cierra el panel de fecha usando múltiples estrategias."""
            try:
                # Estrategia 1: Presionar ESC para cerrar el panel
                try:
                    from selenium.webdriver.common.keys import Keys
                    body = driver.find_element(By.TAG_NAME, "body")
                    body.send_keys(Keys.ESCAPE)
                    sleep(0.5)
                    logger.debug("   ✓ Panel cerrado con ESC")
                    return True
                except Exception:
                    pass
                
                # Estrategia 2: Esperar a que el panel sea clickable y hacer clic fuera con JavaScript
                try:
                    # Buscar un elemento fuera del panel para hacer clic
                    filter_panel = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#allTask_tab")))
                    # Usar JavaScript click para evitar interceptores
                    driver.execute_script("arguments[0].click();", filter_panel)
                    sleep(0.5)
                    logger.debug("   ✓ Panel cerrado con JavaScript click")
                    return True
                except Exception:
                    pass
                
                # Estrategia 3: Esperar a que el panel sea clickable y usar ActionChains
                try:
                    from selenium.webdriver.common.action_chains import ActionChains
                    filter_panel = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#allTask_tab")))
                    ActionChains(driver).move_to_element(filter_panel).click().perform()
                    sleep(0.5)
                    logger.debug("   ✓ Panel cerrado con ActionChains")
                    return True
                except Exception:
                    pass
                
                # Estrategia 4: Cerrar panel directamente con JavaScript
                try:
                    driver.execute_script("""
                        // Buscar y cerrar cualquier panel de fecha abierto
                        const panels = document.querySelectorAll('.el-picker-panel');
                        panels.forEach(panel => {
                            if (panel.style.display !== 'none') {
                                const closeBtn = panel.querySelector('.el-picker-panel__close-btn, .el-date-picker__header-btn');
                                if (closeBtn) closeBtn.click();
                            }
                        });
                        // También disparar evento ESC
                        document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', keyCode: 27 }));
                    """)
                    sleep(0.5)
                    logger.debug("   ✓ Panel cerrado con JavaScript directo")
                    return True
                except Exception:
                    pass
                
                logger.warning("   ⚠️ No se pudo cerrar el panel con ninguna estrategia")
                return False
            except Exception as e:
                logger.warning(f"   ⚠️ Error al cerrar panel: {e}")
                return False
        
        # Función helper para esperar a que el panel de fecha se cierre completamente
        def wait_for_date_picker_closed(timeout=5):
            """Espera a que el panel de fecha se cierre completamente."""
            try:
                wait_closed = WebDriverWait(driver, timeout)
                # Verificar si hay paneles visibles
                def no_visible_panels(driver):
                    try:
                        panels = driver.find_elements(By.CSS_SELECTOR, ".el-picker-panel")
                        visible_panels = [p for p in panels if p.is_displayed()]
                        return len(visible_panels) == 0
                    except Exception:
                        return True  # Si hay error, asumir que está cerrado
                
                wait_closed.until(no_visible_panels)
                logger.debug("   ✓ Panel de fecha cerrado completamente")
                return True
            except Exception:
                # Fallback: verificar manualmente
                try:
                    panels = driver.find_elements(By.CSS_SELECTOR, ".el-picker-panel")
                    visible_panels = [p for p in panels if p.is_displayed()]
                    if not visible_panels:
                        logger.debug("   ✓ No hay paneles de fecha visibles")
                        return True
                except Exception:
                    pass
                # Si no podemos verificar, asumir que está cerrado después del timeout
                logger.debug("   ⚠️ No se pudo verificar el cierre del panel, continuando...")
                return True
        
        # Función helper para verificar que la fecha se guardó correctamente
        def verify_date_applied(input_element, expected_date_str, field_name):
            """Verifica que la fecha se guardó correctamente en el input."""
            try:
                # Esperar un poco para que se actualice
                sleep(0.5)
                
                # Obtener el valor del input usando JavaScript
                actual_value = driver.execute_script("return arguments[0].value || '';", input_element)
                
                # También verificar el valor del componente Vue si existe
                vue_value = driver.execute_script("""
                    const input = arguments[0];
                    let parent = input.parentElement;
                    while (parent) {
                        if (parent.__vue__) {
                            return parent.__vue__.value || parent.__vue__.displayValue || '';
                        }
                        parent = parent.parentElement;
                    }
                    return '';
                """, input_element)
                
                # Normalizar fechas para comparación (solo la parte de fecha, sin hora)
                expected_normalized = expected_date_str.replace('/', '-')
                actual_normalized = actual_value.replace('/', '-')
                vue_normalized = vue_value.replace('/', '-')
                
                # Verificar si alguna de las fechas coincide
                date_applied = (
                    expected_normalized in actual_normalized or 
                    actual_normalized in expected_normalized or
                    expected_normalized in vue_normalized or
                    vue_normalized in expected_normalized
                )
                
                if date_applied:
                    logger.debug(f"   ✅ Fecha {field_name} verificada: Input='{actual_value}', Vue='{vue_value}'")
                    return True
                else:
                    logger.warning(f"   ⚠️ Fecha {field_name} no coincide: Esperada='{expected_date_str}', Input='{actual_value}', Vue='{vue_value}'")
                    return False
            except Exception as e:
                logger.warning(f"   ⚠️ Error al verificar fecha {field_name}: {e}")
                return False
        
        # Aplicar fechas usando los elementos encontrados (CREATE TIME)
        # Paso 1: Aplicar fecha DESDE y confirmar
        try:
            logger.info("📅 Aplicando fecha DESDE...")
            # Usar JavaScript click para abrir panel (mejor compatibilidad en headless)
            driver.execute_script("arguments[0].click();", input_from_elem)
            sleep(1.0)  # Esperar a que el panel se abra completamente
            
            # Aplicar la fecha con JavaScript
            apply_date_to_vue_component(input_from_elem, date_from_datetime, date_from_str, "DESDE")
            sleep(1.0)  # Esperar a que se actualice
            
            # Hacer clic en Confirmar - CRÍTICO: debe funcionar
            confirm_clicked = click_confirm_date_picker()
            if not confirm_clicked:
                logger.warning("⚠️ No se pudo hacer clic en 'Confirmar' para fecha DESDE, intentando cerrar panel...")
                # Intentar cerrar el panel y verificar si la fecha se aplicó de todas formas
                close_date_picker_panel()
                sleep(1.0)
                if not verify_date_applied(input_from_elem, date_from_str, "DESDE"):
                    raise RuntimeError("No se pudo aplicar la fecha DESDE: botón Confirmar no funcionó y fecha no se guardó")
            else:
                # Esperar a que el panel se cierre y verificar
                wait_for_date_picker_closed(timeout=5)
                sleep(1.0)
                
                # Verificar que la fecha se guardó correctamente
                if not verify_date_applied(input_from_elem, date_from_str, "DESDE"):
                    logger.warning("⚠️ Fecha DESDE aplicada pero no se verificó correctamente")
                else:
                    logger.info("✓ Fecha DESDE confirmada y verificada")
        except Exception as e:
            logger.error(f"❌ Error al aplicar fecha DESDE: {e}")
            # Intentar cerrar el panel antes de fallar
            try:
                close_date_picker_panel()
            except Exception:
                pass
            raise RuntimeError("No se pudo aplicar la fecha DESDE") from e
        
        # Paso 2: Aplicar fecha HASTA y confirmar
        try:
            logger.info("📅 Aplicando fecha HASTA...")
            wait_for_date_picker_closed(timeout=3)
            sleep(1.0)  # Asegurar que el panel anterior esté cerrado
            
            # Usar JavaScript click para abrir panel (mejor compatibilidad en headless)
            driver.execute_script("arguments[0].click();", input_to_elem)
            sleep(1.0)  # Esperar a que el panel se abra completamente
            
            # Aplicar la fecha con JavaScript
            apply_date_to_vue_component(input_to_elem, date_to_datetime, date_to_str, "HASTA")
            sleep(1.0)  # Esperar a que se actualice
            
            # Hacer clic en Confirmar - CRÍTICO: debe funcionar
            confirm_clicked = click_confirm_date_picker()
            if not confirm_clicked:
                logger.warning("⚠️ No se pudo hacer clic en 'Confirmar' para fecha HASTA, intentando cerrar panel...")
                # Intentar cerrar el panel y verificar si la fecha se aplicó de todas formas
                close_date_picker_panel()
                sleep(1.0)
                if not verify_date_applied(input_to_elem, date_to_str, "HASTA"):
                    raise RuntimeError("No se pudo aplicar la fecha HASTA: botón Confirmar no funcionó y fecha no se guardó")
            else:
                # Esperar a que el panel se cierre y verificar
                wait_for_date_picker_closed(timeout=5)
                sleep(1.0)
                
                # Verificar que la fecha se guardó correctamente
                if not verify_date_applied(input_to_elem, date_to_str, "HASTA"):
                    logger.warning("⚠️ Fecha HASTA aplicada pero no se verificó correctamente")
                else:
                    logger.info("✓ Fecha HASTA confirmada y verificada")
        except Exception as e:
            logger.error(f"❌ Error al aplicar fecha HASTA: {e}")
            # Intentar cerrar el panel antes de fallar
            try:
                close_date_picker_panel()
            except Exception:
                pass
            raise RuntimeError("No se pudo aplicar la fecha HASTA") from e
        
        # Verificación final: ambas fechas deben estar guardadas
        logger.debug("🔍 Verificación final de fechas aplicadas...")
        from_done = verify_date_applied(input_from_elem, date_from_str, "DESDE (final)")
        to_done = verify_date_applied(input_to_elem, date_to_str, "HASTA (final)")
        
        if from_done and to_done:
            logger.info("✅ Ambas fechas verificadas correctamente")
        else:
            logger.warning(f"⚠️ Verificación final: DESDE={from_done}, HASTA={to_done}")
        
        # CRÍTICO: Sincronizar fechas con el formulario antes de aplicar filtros
        # Esto asegura que los valores se envíen correctamente cuando se hace clic en "Filtrar"
        logger.debug("🔄 Sincronizando fechas con el formulario y modelo Vue padre...")
        sync_result = driver.execute_script("""
            const dateFrom = arguments[0];
            const dateTo = arguments[1];
            const dateFromDatetime = arguments[2];
            const dateToDatetime = arguments[3];
            
            let synced = false;
            
            // Buscar el componente Vue padre que maneja los filtros
            // Buscar en el panel de filtros o drawer
            const filterPanel = document.querySelector('[class*="filter-panel"], [id*="filter"], [class*="el-drawer"], [class*="drawer"]');
            const createtimeRow = document.querySelector('#createtimeRow');
            
            // Buscar componentes Vue padre que puedan manejar el estado de filtros
            let filterVueComponent = null;
            
            // Estrategia 1: Buscar componente Vue en el panel de filtros
            if (filterPanel && filterPanel.__vue__) {
                filterVueComponent = filterPanel.__vue__;
            }
            
            // Estrategia 2: Buscar componente Vue padre de createtimeRow
            if (!filterVueComponent && createtimeRow) {
                let parent = createtimeRow.parentElement;
                let depth = 0;
                while (parent && depth < 10) {
                    if (parent.__vue__) {
                        const vue = parent.__vue__;
                        // Buscar componente que tenga datos de filtros
                        if (vue.$data && (vue.$data.filterForm || vue.$data.filters || vue.$data.formData)) {
                            filterVueComponent = vue;
                            break;
                        }
                    }
                    parent = parent.parentElement;
                    depth++;
                }
            }
            
            // Estrategia 3: Buscar en toda la página el componente Vue que maneja filtros
            if (!filterVueComponent) {
                const allElements = document.querySelectorAll('*');
                for (let elem of allElements) {
                    if (elem.__vue__) {
                        const vue = elem.__vue__;
                        // Buscar componente con datos de filtros o formulario
                        if (vue.$data) {
                            const data = vue.$data;
                            // Buscar propiedades comunes de formularios de filtros
                            if (data.filterForm || data.filters || data.formData || 
                                data.createTime || data.createTimeFrom || data.createTimeTo ||
                                (data.form && typeof data.form === 'object')) {
                                filterVueComponent = vue;
                                break;
                            }
                        }
                    }
                }
            }
            
            // Si encontramos el componente Vue padre, actualizar su modelo
            if (filterVueComponent) {
                const data = filterVueComponent.$data;
                
                // CRÍTICO: Crear filterForm si no existe
                if (!data.filterForm) {
                    if (filterVueComponent.$set) {
                        filterVueComponent.$set(data, 'filterForm', {});
                        synced = true;
                    } else {
                        data.filterForm = {};
                    }
                }
                
                // Intentar actualizar diferentes propiedades posibles
                const possibleProps = [
                    'createTime', 'createTimeFrom', 'createTimeTo',
                    'filterForm', 'filters', 'formData', 'form'
                ];
                
                for (let prop of possibleProps) {
                    if (data[prop]) {
                        if (typeof data[prop] === 'object') {
                            // Si es un objeto, actualizar propiedades dentro
                            if (data[prop].createTimeFrom !== undefined || prop === 'filterForm') {
                                if (filterVueComponent.$set) {
                                    filterVueComponent.$set(data[prop], 'createTimeFrom', dateFromDatetime);
                                } else {
                                    data[prop].createTimeFrom = dateFromDatetime;
                                }
                                synced = true;
                            }
                            if (data[prop].createTimeTo !== undefined || prop === 'filterForm') {
                                if (filterVueComponent.$set) {
                                    filterVueComponent.$set(data[prop], 'createTimeTo', dateToDatetime);
                                } else {
                                    data[prop].createTimeTo = dateToDatetime;
                                }
                                synced = true;
                            }
                            if (data[prop].from !== undefined || prop === 'filterForm') {
                                if (filterVueComponent.$set) {
                                    filterVueComponent.$set(data[prop], 'from', dateFromDatetime);
                                } else {
                                    data[prop].from = dateFromDatetime;
                                }
                                synced = true;
                            }
                            if (data[prop].to !== undefined || prop === 'filterForm') {
                                if (filterVueComponent.$set) {
                                    filterVueComponent.$set(data[prop], 'to', dateToDatetime);
                                } else {
                                    data[prop].to = dateToDatetime;
                                }
                                synced = true;
                            }
                        }
                    }
                }
                
                // CRÍTICO: Asegurar que filterForm tenga las fechas (si existe o se creó)
                if (data.filterForm) {
                    if (filterVueComponent.$set) {
                        filterVueComponent.$set(data.filterForm, 'createTimeFrom', dateFromDatetime);
                        filterVueComponent.$set(data.filterForm, 'createTimeTo', dateToDatetime);
                        filterVueComponent.$set(data.filterForm, 'from', dateFromDatetime);
                        filterVueComponent.$set(data.filterForm, 'to', dateToDatetime);
                    } else {
                        data.filterForm.createTimeFrom = dateFromDatetime;
                        data.filterForm.createTimeTo = dateToDatetime;
                        data.filterForm.from = dateFromDatetime;
                        data.filterForm.to = dateToDatetime;
                    }
                    synced = true;
                }
                
                // También actualizar directamente si existen estas propiedades
                if (filterVueComponent.$set) {
                    if (data.createTimeFrom !== undefined) {
                        filterVueComponent.$set(filterVueComponent, 'createTimeFrom', dateFromDatetime);
                        synced = true;
                    }
                    if (data.createTimeTo !== undefined) {
                        filterVueComponent.$set(filterVueComponent, 'createTimeTo', dateToDatetime);
                        synced = true;
                    }
                    // También crear si no existen
                    if (data.createTimeFrom === undefined) {
                        filterVueComponent.$set(filterVueComponent, 'createTimeFrom', dateFromDatetime);
                        synced = true;
                    }
                    if (data.createTimeTo === undefined) {
                        filterVueComponent.$set(filterVueComponent, 'createTimeTo', dateToDatetime);
                        synced = true;
                    }
                }
                
                // Forzar actualización del componente
                if (filterVueComponent.$forceUpdate) {
                    filterVueComponent.$forceUpdate();
                }
                
                // También actualizar componentes padre si existen
                let parent = filterVueComponent.$parent;
                while (parent && parent.$forceUpdate) {
                    parent.$forceUpdate();
                    parent = parent.$parent;
                }
            }
            
            // También actualizar los inputs directamente para asegurar que tienen los valores correctos
            const inputs = document.querySelectorAll('#createtimeRow input');
            if (inputs.length >= 2) {
                const fromInput = inputs[0];
                const toInput = inputs[1];
                
                // Actualizar valores usando setter nativo
                const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                nativeInputValueSetter.call(fromInput, dateFrom);
                nativeInputValueSetter.call(toInput, dateTo);
                
                // Disparar eventos
                ['focus', 'input', 'change', 'blur'].forEach(eventType => {
                    fromInput.dispatchEvent(new Event(eventType, { bubbles: true }));
                    toInput.dispatchEvent(new Event(eventType, { bubbles: true }));
                });
            }
            
            return { synced: synced, foundComponent: filterVueComponent !== null };
        """, date_from_str, date_to_str, date_from_datetime, date_to_datetime)
        
        if sync_result and sync_result.get('synced'):
            logger.info("✅ Fechas sincronizadas con el modelo Vue padre del formulario")
        elif sync_result and sync_result.get('foundComponent'):
            logger.warning("⚠️ Componente Vue padre encontrado pero no se pudo sincronizar completamente")
        else:
            logger.warning("⚠️ No se encontró componente Vue padre del formulario, usando sincronización básica")
        
        sleep(1.5)  # Esperar más tiempo para que se sincronice completamente
        
        # CRÍTICO: Hacer blur en los inputs de fecha para asegurar que Vue los registre
        logger.debug("🔄 Haciendo blur en inputs de fecha para activar registro en Vue...")
        try:
            inputs = driver.find_elements(By.CSS_SELECTOR, "#createtimeRow input")
            if len(inputs) >= 2:
                # Hacer blur en ambos inputs para activar el registro en Vue
                driver.execute_script("arguments[0].blur();", inputs[0])
                sleep(0.3)
                driver.execute_script("arguments[0].blur();", inputs[1])
                sleep(0.5)
                logger.debug("✓ Blur aplicado en inputs de fecha")
        except Exception as e:
            logger.warning(f"⚠️ No se pudo hacer blur en inputs: {e}")
        
        sleep(1.0)  # Esperar adicional después del blur
    elif settings.date_mode == 2:
        logger.info("📅 Seleccionando rango rápido: Último mes")
        driver.find_element(By.XPATH, '//*[@id="createtimeRow"]/div[2]/div[2]/div/div[1]/label[3]').click()
        sleep(1)
    else:
        raise RuntimeError("DATE_MODE no válido. Usa 1 o 2.")


def _apply_filters(driver) -> None:
    """Aplica los filtros haciendo clic en el botón "Filtrar" del panel de filtros.

    Primero hace hover sobre el split button para habilitarlo, luego hace clic
    en el botón "Filtrar" que está dentro del panel de filtros.
    """
    logger.info("🔧 Aplicando filtros...")
    
    # CRÍTICO: Verificar estado de fechas ANTES de hacer clic en "Filtrar"
    logger.info("🔍 Verificando estado de fechas antes de aplicar filtros...")
    date_status = driver.execute_script("""
        const createtimeRow = document.querySelector('#createtimeRow');
        if (!createtimeRow) return { found: false };
        
        const inputs = createtimeRow.querySelectorAll('input');
        if (inputs.length < 2) return { found: false, inputs: inputs.length };
        
        const fromInput = inputs[0];
        const toInput = inputs[1];
        
        // Obtener valores de inputs
        const fromValue = fromInput.value || '';
        const toValue = toInput.value || '';
        
        // Buscar componentes Vue de los inputs
        let fromVue = null;
        let toVue = null;
        
        let parent = fromInput.parentElement;
        let depth = 0;
        while (parent && depth < 10 && !fromVue) {
            if (parent.__vue__) {
                fromVue = parent.__vue__;
                break;
            }
            parent = parent.parentElement;
            depth++;
        }
        
        parent = toInput.parentElement;
        depth = 0;
        while (parent && depth < 10 && !toVue) {
            if (parent.__vue__) {
                toVue = parent.__vue__;
                break;
            }
            parent = parent.parentElement;
            depth++;
        }
        
        // Obtener valores de componentes Vue
        const fromVueValue = fromVue ? (fromVue.value || fromVue.displayValue || '') : '';
        const toVueValue = toVue ? (toVue.value || toVue.displayValue || '') : '';
        
        // Buscar componente Vue padre que maneja filtros
        let filterVue = null;
        parent = createtimeRow.parentElement;
        depth = 0;
        while (parent && depth < 15) {
            if (parent.__vue__) {
                const vue = parent.__vue__;
                if (vue.$data && (vue.$data.filterForm || vue.$data.filters || vue.$data.formData)) {
                    filterVue = vue;
                    break;
                }
            }
            parent = parent.parentElement;
            depth++;
        }
        
        // Obtener valores del componente Vue padre si existe
        let filterFormFrom = null;
        let filterFormTo = null;
        if (filterVue && filterVue.$data) {
            const data = filterVue.$data;
            if (data.filterForm && data.filterForm.createTimeFrom) {
                filterFormFrom = data.filterForm.createTimeFrom;
            }
            if (data.filterForm && data.filterForm.createTimeTo) {
                filterFormTo = data.filterForm.createTimeTo;
            }
            if (data.createTimeFrom) filterFormFrom = data.createTimeFrom;
            if (data.createTimeTo) filterFormTo = data.createTimeTo;
        }
        
        return {
            found: true,
            inputFrom: fromValue,
            inputTo: toValue,
            vueFrom: fromVueValue,
            vueTo: toVueValue,
            filterFormFrom: filterFormFrom,
            filterFormTo: filterFormTo,
            hasFilterVue: filterVue !== null
        };
    """)
    
    if date_status and date_status.get('found'):
        logger.info(f"   📅 Estado de fechas antes de filtrar:")
        logger.info(f"      Input DESDE: {date_status.get('inputFrom', 'N/A')}")
        logger.info(f"      Input HASTA: {date_status.get('inputTo', 'N/A')}")
        logger.info(f"      Vue DESDE: {date_status.get('vueFrom', 'N/A')}")
        logger.info(f"      Vue HASTA: {date_status.get('vueTo', 'N/A')}")
        if date_status.get('hasFilterVue'):
            logger.info(f"      Form DESDE: {date_status.get('filterFormFrom', 'N/A')}")
            logger.info(f"      Form HASTA: {date_status.get('filterFormTo', 'N/A')}")
        
        # Verificar si las fechas están vacías
        if not date_status.get('inputFrom') or not date_status.get('inputTo'):
            logger.warning("⚠️ ⚠️ ⚠️ ADVERTENCIA: Las fechas están vacías en los inputs antes de filtrar!")
            logger.warning("   Esto significa que los filtros de fecha NO se aplicarán")
    
    # Paso 1: Hover sobre el split button para habilitarlo
    wait = WebDriverWait(driver, 15)
    try:
        element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#allTask_tab .el-button:nth-child(3)")))
        ActionChains(driver).move_to_element(element).perform()
        sleep(2.0)  # Aumentado para headless
        logger.debug("✓ Hover sobre split button realizado")
    except Exception as e:
        logger.warning(f"⚠️ No se pudo hacer hover sobre split button: {e}")
    
    # Paso 2: Buscar y hacer clic en el botón "Filtrar" del panel de filtros
    try:
        wait = WebDriverWait(driver, 15)
        # Esperar explícitamente a que el botón esté disponible
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.ows_filter_filter")))
        
        # Buscar botón "Filtrar" con clase ows_filter_filter que esté visible
        filter_buttons = driver.find_elements(By.CSS_SELECTOR, "button.ows_filter_filter")
        filter_button = None
        
        for btn in filter_buttons:
            try:
                # En headless, verificar con JavaScript si está visible
                is_visible = driver.execute_script("""
                    var elem = arguments[0];
                    if (!elem) return false;
                    var style = window.getComputedStyle(elem);
                    return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
                """, btn)
                
                if is_visible and btn.is_enabled() and btn.text.strip() == "Filtrar":
                    filter_button = btn
                    break
            except Exception:
                continue
        
        if filter_button:
            logger.info("   📌 Haciendo clic en botón 'Filtrar' (JavaScript click para compatibilidad)...")
            # Usar JavaScript click para mayor compatibilidad en headless
            driver.execute_script("arguments[0].click();", filter_button)
            sleep(3.0)  # Aumentado para headless
            logger.info("✓ Filtros aplicados correctamente")
        else:
            logger.warning("⚠️ No se encontró el botón 'Filtrar' visible, usando fallback...")
            # Fallback con JavaScript click
            wait = WebDriverWait(driver, 15)
            element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#allTask_tab .el-button:nth-child(3)")))
            driver.execute_script("arguments[0].click();", element)
            sleep(3.0)
    except Exception as e:
        logger.warning(f"⚠️ Error al hacer clic en botón Filtrar: {e}")
        # Fallback al comportamiento original
        try:
            element = driver.find_element(By.CSS_SELECTOR, "#allTask_tab .el-button:nth-child(3)")
            ActionChains(driver).move_to_element(element).perform()
            sleep(2)
        except Exception as e2:
            logger.error(f"❌ Error en fallback: {e2}")
            raise RuntimeError("No se pudo aplicar los filtros") from e2


def _trigger_export(driver) -> float:
    """Hace click en el botón de exportación y devuelve el timestamp del disparo.

    El timestamp es usado luego por ``_download_export`` para identificar cuál
    de los archivos descargados corresponde al request actual.
    """
    logger.info("📤 Disparando exportación...")
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
    logger.info("🔄 Iniciando monitoreo de estado de exportación...")
    end_states = {"Succeed", "Failed", "Aborted", "Waiting", "Concurrent Waiting"}
    deadline = time.time() + timeout_seconds
    attempt = 0

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
    logger.info("📥 Preparando descarga...")
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
    download_dir = Path(settings.download_path).resolve()
    download_dir.mkdir(parents=True, exist_ok=True)

    browser_kwargs: Dict[str, Any] = {
        "download_path": str(download_dir),
        "headless": settings.headless if headless is None else headless,
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
