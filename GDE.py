import sys
import traceback
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time
from pathlib import Path

# Importar configuración desde config.py
from config import (
    USERNAME, PASSWORD, DOWNLOAD_PATH, MAX_IFRAME_ATTEMPTS, 
    MAX_STATUS_ATTEMPTS, OPTIONS_TO_SELECT, DATE_MODE, DATE_FROM, DATE_TO
)

# Importar módulos del scraper
from src.auth_manager import AuthManager
from src.browser_manager import BrowserManager
from src.iframe_manager import IframeManager
from src.filter_manager import FilterManager

def main():
    # === CONFIGURACIÓN DEL NAVEGADOR ===
    browser_manager = BrowserManager(download_path=DOWNLOAD_PATH)
    driver, wait = browser_manager.create_driver()

    try:
        # === LOGIN ===
        auth_manager = AuthManager(driver)
        login_success = auth_manager.login(USERNAME, PASSWORD)
        
        if not login_success:
            print("❌ Error: No se pudo realizar el login. Terminando ejecución.")
            return

        # === ESPERAR Y CAMBIAR AL IFRAME PRINCIPAL ===
        iframe_manager = IframeManager(driver)
        iframe_encontrado = iframe_manager.find_main_iframe(max_attempts=MAX_IFRAME_ATTEMPTS)

        # === ESPERAR Y ABRIR PANEL DE FILTROS ===
        filter_manager = FilterManager(driver, wait)
        
        # Esperar a que los filtros estén listos
        filter_manager.wait_for_filters_ready()
        
        # Abrir el panel de filtros usando el método complejo
        filter_manager.open_filter_panel(method="complex")


      # Click clear
        driver.find_element(By.XPATH, '//*[@id="allTask_tab"]/form/div[2]/div/div/div[2]/button[2]').click()
        print("✓ se limpio el filtro")
        sleep(1)

        try:
            # Abrir desplegable
            print("📋 Abriendo desplegable...")
            driver.find_element(By.CSS_SELECTOR, "#all_taskType .el-select__caret").click()
            sleep(1)
            
            # Seleccionar por XPath con title
            for opcion in OPTIONS_TO_SELECT:
                xpath = f"//li[contains(@class, 'el-select-dropdown__item') and @title='{opcion}']"
                wait.until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
                print(f"✓ {opcion}")
                sleep(0.3)
            
            print("✅ Completado")

        except Exception as e:
            print(f"❌ Error: {e}")


        # === APLICAR FILTROS SEGÚN VARIABLE ===
        if DATE_MODE == 1:
            # === APLICAR FILTRO DE FECHA MANUAL (DESDE) ===
            script_fecha = f'''
                const xpath = '//*[@id="closetimeRow"]/div[2]/div[2]/div/div/div[2]/div[1]/input';
                const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                const input = result.singleNodeValue;

                if (input) {{
                    input.value = "{DATE_FROM}";
                    input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                    return true;
                }}
                return false;
            '''
            fecha_aplicada = driver.execute_script(script_fecha)
            if fecha_aplicada:
                print(f"✓ Fecha DESDE aplicada: {DATE_FROM}")
            else:
                print("⚠ No se pudo aplicar la fecha DESDE.")
            
            sleep(0.5)
            
            # === APLICAR FILTRO DE FECHA MANUAL (HASTA) ===
            script_fecha_hasta = f'''
                const xpath = '//*[@id="closetimeRow"]/div[2]/div[3]/div/div/div[2]/div[1]/input';
                const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                const input = result.singleNodeValue;

                if (input) {{
                    input.value = "{DATE_TO}";
                    input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                    input.dispatchEvent(new Event('change', {{ bubbles: true }}));
                    return true;
                }}
                return false;
            '''
            fecha_hasta_aplicada = driver.execute_script(script_fecha_hasta)
            if fecha_hasta_aplicada:
                print(f"✓ Fecha HASTA aplicada: {DATE_TO}")
            else:
                print("⚠ No se pudo aplicar la fecha HASTA.")
            
            input("Presiona Enter para continuar...")

        elif DATE_MODE == 2:
            # === USAR FILTRO RÁPIDO: ÚLTIMO MES ===
            driver.find_element(By.XPATH, '//*[@id="createtimeRow"]/div[2]/div[2]/div/div[1]/label[3]').click()
            print("✓ Se asignó último mes")
            sleep(1)

        else:
            print("⚠ Variable no válida. Usa 1 o 2.")
        
        # Aplica filtro
        element = driver.find_element(By.CSS_SELECTOR, "#allTask_tab .el-button:nth-child(3)")
        actions = ActionChains(driver)
        actions.move_to_element(element).perform()
        print("✓ Aplica filtro")
        sleep(2)
        

       
        # Click en exportar 
        driver.find_element(By.CSS_SELECTOR, "#test > .sdm_splitbutton_text").click()
        print("✓ exportar ")
        sleep(1)


        
        # === VOLVER AL CONTENIDO PRINCIPAL ===
        iframe_manager.switch_to_default_content()
        
        # Cerrar modal/panel
        driver.find_element(By.CSS_SELECTOR, ".el-icon-close:nth-child(2)").click()
        print("✓ Cerrar modal/panel ")
        sleep(1)
        
        # Click en sexto item del sidebar (icono)
        driver.find_element(By.CSS_SELECTOR, ".el-row:nth-child(6) > .side-item-icon").click()
        print("✓ Click en sidebar item 6")
        sleep(1)

        # Click en level-1
        driver.find_element(By.CSS_SELECTOR, ".level-1").click()
        print("✓ Click en level-1")
        sleep(1)

        # === CAMBIAR AL SEGUNDO IFRAME ===
        iframe_manager.switch_to_iframe(1)
        sleep(1)

        # === BUCLE DE REFRESH CON VERIFICACIÓN DE ESTADO ===
        print("🔄 Iniciando proceso de carga...")
        intentoc = 0

        while intentoc < MAX_STATUS_ATTEMPTS:
            try:
                # Click en botón Refresh
                driver.find_element(By.CSS_SELECTOR, "span.button_icon.btnIcon[style*='refresh']").click()
                print(f"🔄 Refresh {intentoc + 1}/{MAX_STATUS_ATTEMPTS}")
                sleep(3)  # Esperar que se actualice la tabla
                
                # Obtener el estado actual
                estado = driver.find_element(By.XPATH, '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]/td[3]/div/span').text.strip()
                print(f"   📊 Estado: {estado}")
                
                # Verificar el estado
                if estado == "Succeed":
                    print("✅ ¡Carga completada exitosamente!")
                    break
                elif estado == "Failed":
                    print("❌ ¡Carga fallida!")
                    break
                elif estado == "Aborted":
                    print("🛑 ¡Proceso abortado!")
                    break
                elif estado == "Waiting":
                    print("⏸️ ¡Proceso Waiting!")
                    break
                elif estado == "Concurrent Waiting":
                    print("⏸️ ¡Proceso Concurrent Waiting!")
                    break
                elif estado == "Running":
                    print("   ⏳ Aún procesando... esperando 10 segundos")
                    sleep(10)
                    intentoc += 1
                else:
                    print(f"   ⚠️ Estado: '{estado}' - Continuando...")
                    sleep(10)
                    intentoc += 1
            
            except Exception as e:
                print(f"   ⚠️ Error al verificar estado: {e}")
                sleep(10)
                intentoc += 1

        if intentoc >= MAX_STATUS_ATTEMPTS:
            print("⏱️ Tiempo máximo de espera alcanzado")
        

        # === DESCARGAR ARCHIVO ===
        print("📥 Iniciando descarga...")

        # Contar archivos antes de descargar
        archivos_antes = set(os.listdir(DOWNLOAD_PATH))

        # Click en botón de descarga
        driver.find_element(By.XPATH, '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]/td[11]/div/div[3]').click()
        print("✓ Click en botón de descarga")

        # Esperar que aparezca el archivo
        print("⏳ Esperando descarga...")
        timeout = 60  # Esperar máximo 60 segundos
        inicio = time.time()

        while time.time() - inicio < timeout:
            archivos_ahora = set(os.listdir(DOWNLOAD_PATH))
            archivos_nuevos = archivos_ahora - archivos_antes
            
            # Filtrar archivos temporales de Chrome
            archivos_completos = [f for f in archivos_nuevos if not f.endswith('.crdownload')]
            
            if archivos_completos:
                archivo_descargado = archivos_completos[0]
                print(f"✅ Descarga completada: {archivo_descargado}")
                print(f"📂 Ruta: {os.path.join(DOWNLOAD_PATH, archivo_descargado)}")
                break
            
            sleep(2)
        else:
            print("⏱️ Timeout: La descarga tardó más de 60 segundos")
 
    except Exception:
        print("❌ Error durante el proceso:")
        traceback.print_exc()
    finally:
        input("Presiona Enter para cerrar el navegador...")
        browser_manager.close_driver()
    
if __name__ == "__main__":
    main()