"""
Helpers compartidos para interacciones con componentes Vue.js en el portal Integratel.

Estas funciones facilitan la manipulación de componentes Vue.js (Element UI) desde Selenium,
especialmente para date pickers y otros componentes que requieren actualización del estado interno de Vue.
"""

from __future__ import annotations

from typing import Optional, Any, Dict
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement


def find_vue_component(driver: WebDriver, element: WebElement) -> Optional[Any]:
    """
    Busca el componente Vue padre de un elemento HTML.
    
    Args:
        driver: Instancia de WebDriver
        element: Elemento WebElement de Selenium
        
    Returns:
        Componente Vue encontrado o None
    """
    result = driver.execute_script("""
        const element = arguments[0];
        let parent = element.parentElement;
        while (parent && parent !== document.body) {
            if (parent.__vue__) {
                return parent.__vue__;
            }
            parent = parent.parentElement;
        }
        return null;
    """, element)
    return result


def apply_vue_date_value(driver: WebDriver, vue_component: Any, value: str) -> None:
    """
    Actualiza el valor de un componente Vue.js date picker.
    
    Args:
        driver: Instancia de WebDriver
        vue_component: Componente Vue encontrado
        value: Valor de fecha en formato 'YYYY-MM-DD HH:MM:SS'
    """
    if not vue_component:
        return
    
    # Vue 2
    if hasattr(vue_component, '$set'):
        driver.execute_script("""
            const component = arguments[0];
            const value = arguments[1];
            component.$set(component, 'value', value);
            component.$set(component, 'displayValue', value);
            if (component.$forceUpdate) component.$forceUpdate();
        """, vue_component, value)
    # Vue 3 o directo
    elif hasattr(vue_component, 'value'):
        driver.execute_script("""
            const component = arguments[0];
            const value = arguments[1];
            component.value = value;
            if (component.displayValue !== undefined) {
                component.displayValue = value;
            }
        """, vue_component, value)
    
    # Emitir eventos
    if hasattr(vue_component, '$emit'):
        driver.execute_script("""
            const component = arguments[0];
            const value = arguments[1];
            component.$emit('input', value);
            component.$emit('change', value);
        """, vue_component, value)


def inject_date_via_javascript(
    driver: WebDriver,
    input_element: WebElement,
    date_value: str,
    date_display: str,
) -> Dict[str, Any]:
    """
    Inyecta una fecha en un input de date picker usando JavaScript y actualiza el componente Vue.
    
    Args:
        driver: Instancia de WebDriver
        input_element: Elemento input del date picker
        date_value: Valor completo con hora (ej: '2025-09-27 00:00:00')
        date_display: Valor de fecha sin hora (ej: '2025-09-27')
        
    Returns:
        Diccionario con información del resultado (success, inputValue, vueValue, hasVue)
    """
    result = driver.execute_script("""
        const input = arguments[0];
        const dateWithTime = arguments[1];
        
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
        
        // Si encontramos Vue, actualizar
        if (vueComponent) {
            if (vueComponent.$set) {
                vueComponent.$set(vueComponent, 'value', dateWithTime);
                vueComponent.$set(vueComponent, 'displayValue', dateWithTime);
                if (vueComponent.$forceUpdate) vueComponent.$forceUpdate();
            } else if (vueComponent.value !== undefined) {
                vueComponent.value = dateWithTime;
                if (vueComponent.displayValue !== undefined) {
                    vueComponent.displayValue = dateWithTime;
                }
            }
            
            if (vueComponent.$emit) {
                vueComponent.$emit('input', dateWithTime);
                vueComponent.$emit('change', dateWithTime);
            }
        }
        
        // Actualizar input usando setter nativo
        try {
            const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
                window.HTMLInputElement.prototype, 'value').set;
            nativeInputValueSetter.call(input, dateWithTime);
        } catch (e) {
            input.value = dateWithTime;
        }
        
        // Disparar eventos en el input
        input.dispatchEvent(new Event('focus', { bubbles: true }));
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
        
        // Presionar Enter para confirmar la fecha
        const enterEvent = new KeyboardEvent('keydown', {
            key: 'Enter',
            code: 'Enter',
            keyCode: 13,
            which: 13,
            bubbles: true
        });
        input.dispatchEvent(enterEvent);
        input.dispatchEvent(new Event('blur', { bubbles: true }));
        
        return {
            success: true,
            inputValue: input.value,
            vueValue: vueComponent ? (vueComponent.value || vueComponent.displayValue || '') : '',
            hasVue: !!vueComponent
        };
    """, input_element, date_value)
    
    return result


def dispatch_vue_events(driver: WebDriver, vue_component: Any, events: list[str]) -> None:
    """
    Dispara eventos en un componente Vue.js.
    
    Args:
        driver: Instancia de WebDriver
        vue_component: Componente Vue
        events: Lista de nombres de eventos a disparar
    """
    if not vue_component or not hasattr(vue_component, '$emit'):
        return
    
    for event_name in events:
        driver.execute_script("""
            const component = arguments[0];
            const eventName = arguments[1];
            component.$emit(eventName);
        """, vue_component, event_name)

