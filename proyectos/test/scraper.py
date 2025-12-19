import sys
import traceback
import logging
import shutil
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import os
import time
from pathlib import Path
from datetime import datetime, timedelta  # fechas

# Configurar logger (si no existe, se crea uno básico)
logger = logging.getLogger(__name__)
if not logger.handlers:
    # Si no hay handlers configurados, usar un handler básico
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# === CREDENCIALES (usa variables de entorno si existen) ===
USERNAME = os.getenv("GDE_USER", "t71522450")
PASSWORD = os.getenv("GDE_PASS", "R@fSh@yder199810")
PROXY = "telefonica01.gp.inet:8080"  # Proxy hardcoded para test

# === VARIABLES DE CONFIGURACIÓN ===
max_intentosf = 120  # Máx 120 intentos * 2s = 4 minutos para iframe de filtros
opciones_a_seleccionar = ["CM", "PM", "PLM"]

# 1: Fechas automáticas (ayer y N días atrás) | 2: Último mes
var = 1

# --- Cálculo automático de fechas ---
def calcular_rango_fechas(dias_atras: int = 15):
hoy = datetime.today().date()
fecha_hasta_date = hoy - timedelta(days=1)
    fecha_desde_date = fecha_hasta_date - timedelta(days=dias_atras)
    fecha_desde_val = fecha_desde_date.strftime("%Y-%m-%d")
    fecha_hasta_val = fecha_hasta_date.strftime("%Y-%m-%d")
    return fecha_desde_val, fecha_hasta_val

# --- Horas para el filtro ---
# Cambia si quieres otro rango horario (ej: 00:00:00 a 23:59:59)
hora_desde = "00:00:00"
hora_hasta = "23:59:59"

max_intentosC = 60  # intentos para verificar estado (con sleeps intermedios)

# ---------- Utilidades ----------
def switch_to_frame_with(driver, css_or_xpath):
    """
    Cambia al iframe que contenga el selector dado; si está en main, se queda.
    Devuelve True si lo encuentra, False en caso contrario.
    """
    driver.switch_to.default_content()
    # Busca en main
    try:
        if css_or_xpath.startswith("//"):
            driver.find_element(By.XPATH, css_or_xpath)
        else:
            driver.find_element(By.CSS_SELECTOR, css_or_xpath)
        return True
    except:
        pass

    # Busca en iframes
    frames = driver.find_elements(By.TAG_NAME, "iframe")
    for f in frames:
        driver.switch_to.default_content()
        driver.switch_to.frame(f)
        try:
            if css_or_xpath.startswith("//"):
                driver.find_element(By.XPATH, css_or_xpath)
            else:
                driver.find_element(By.CSS_SELECTOR, css_or_xpath)
            return True
        except:
            continue

    driver.switch_to.default_content()
    return False


def robust_click(driver, elem):
    """
    Intenta click con varias estrategias.
    """
    try:
        elem.click()
        return True
    except:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elem)
            time.sleep(0.2)
            elem.click()
            return True
        except:
            try:
                driver.execute_script("arguments[0].click();", elem)
                return True
            except:
                return False


def find_first(driver, selectors):
    """
    Dada una lista de selectores (CSS o XPATH), retorna el primer elemento encontrado y el selector.
    """
    for sel in selectors:
        try:
            if sel.startswith("//"):
                el = driver.find_element(By.XPATH, sel)
            else:
                el = driver.find_element(By.CSS_SELECTOR, sel)
            return el, sel
        except:
            continue
    return None, None


def get_driver(ruta_descarga: str) -> webdriver.Chrome:
    """
    Crea y devuelve una instancia de Chrome/Chromium configurada para:
    - Headless (para Airflow / Linux)
    - Proxy (si PROXY está configurado)
    - Descargas en ruta_descarga
    - Idioma del navegador en español (es-ES, es)
    - Detección automática de chromedriver en Linux/Docker
    """
    options = webdriver.ChromeOptions()

    # === MODO HEADLESS / ENTORNO SERVIDOR ===
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    # === IDIOMA DEL NAVEGADOR A ESPAÑOL ===
    options.add_argument("--lang=es-ES")
    options.add_experimental_option(
        "prefs",
        {
            # Idiomas preferidos
            "intl.accept_languages": "es-ES,es",
            # Preferencias de descarga
            "download.default_directory": ruta_descarga,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )

    # === PROXY (si está definido) ===
    proxy = PROXY or os.getenv("PROXY")
    if proxy:
        proxy_arg = proxy if "://" in proxy else f"http://{proxy}"
        options.add_argument(f"--proxy-server={proxy_arg}")
        logger.info(f"Proxy configurado en Chrome: {proxy_arg}")
    else:
        logger.info("Sin proxy configurado en Chrome")

    # === ENTORNO DOCKER / LINUX (Chromium) ===
    if os.path.exists("/usr/bin/chromium"):
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--remote-debugging-port=9222")
        options.add_argument("--disable-setuid-sandbox")
        options.add_argument("--disable-extensions")
        options.binary_location = "/usr/bin/chromium"
        logger.info("Configuración Docker/Chromium aplicada (binary=/usr/bin/chromium)")

    # === BÚSQUEDA DE CHROMEDRIVER ===
    chromedriver_path = None
    if os.path.exists("/usr/bin/chromedriver"):
        chromedriver_path = "/usr/bin/chromedriver"
        logger.info("ChromeDriver del sistema detectado: %s", chromedriver_path)
    elif os.path.exists("/usr/local/bin/chromedriver") and os.path.islink("/usr/local/bin/chromedriver"):
        real_path = os.path.realpath("/usr/local/bin/chromedriver")
        if os.path.exists(real_path):
            chromedriver_path = real_path
            logger.info("Usando archivo real del symlink de ChromeDriver: %s", chromedriver_path)
        else:
            chromedriver_path = "/usr/local/bin/chromedriver"
            logger.info("ChromeDriver encontrado en /usr/local/bin: %s", chromedriver_path)
    elif shutil.which("chromedriver"):
        chromedriver_path = shutil.which("chromedriver")
        logger.info("ChromeDriver encontrado en PATH: %s", chromedriver_path)

    # === CREACIÓN DEL DRIVER ===
    logger.info("Creando instancia de ChromeDriver...")
    service = Service(chromedriver_path) if chromedriver_path else Service()
    driver = webdriver.Chrome(options=options, service=service)
    logger.info("ChromeDriver creado exitosamente")
    return driver


def do_login_robusto(driver, wait, ruta_descarga):
    """
    Realiza el login de forma robusta.
    """
    login_url = (
        "https://30c8-mx.teleows.com/dspcas/login?service="
        "https://30c8-mx.teleows.com/portal/web/rest/sso/"
        "index%3Fori_url%3Dhttps%253A%252F%252F30c8-mx.teleows.com%252Fportal-web%252Fportal%252Fhomepage.html"
    )
    logger.info("Navegando a URL de login...")
    driver.get(login_url)
    logger.info("URL cargada, esperando documento listo...")

    # Espera documento listo
    try:
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        logger.info("Documento listo")
    except Exception as e:
        logger.error("Timeout esperando documento listo: %s", e)
        raise

    # Intenta cerrar posibles overlays/cookies (ignora si no existen)
    for sel in [
        "button#onetrust-accept-btn-handler",
        ".cookie-accept",
        "button[aria-label*='accept']",
        ".el-message-box__btns .el-button--primary",
        "//button[contains(.,'Aceptar') or contains(.,'Accept')]",
    ]:
        try:
            if sel.startswith("//"):
                el = driver.find_element(By.XPATH, sel)
            else:
                el = driver.find_element(By.CSS_SELECTOR, sel)
            robust_click(driver, el)
            break
        except:
            pass

    user_selectors = ["#username", "input[name='username']", "input[id*='user']"]
    pass_selectors = ["#password", "input[name='password']", "input[type='password']"]
    btn_selectors = [
        "#loginButton",
        "button#loginButton",
        "button[name='submit']",
        "input[type='submit']",
        "button[type='submit']",
        "//button[contains(translate(., 'LOGININICIAR', 'logininiciar'),'login') or contains(.,'Iniciar')]",
    ]

    # Asegura contexto correcto buscando input de usuario (en main o en iframes)
    logger.info("Buscando campos de usuario y contraseña...")
    if not switch_to_frame_with(driver, "#username"):
        switch_to_frame_with(driver, "input[name='username']")

    usr, _ = find_first(driver, user_selectors)
    pwd, _ = find_first(driver, pass_selectors)

    if not usr or not pwd:
        # reintento: a veces cambia el iframe luego de cargar
        logger.info("Reintentando búsqueda de campos...")
        switch_to_frame_with(driver, "#username")
        usr, _ = find_first(driver, user_selectors)
        pwd, _ = find_first(driver, pass_selectors)

    if not usr or not pwd:
        logger.error("No se encontraron los campos de usuario/contraseña")
        raise RuntimeError("No se encontraron los campos de usuario/contraseña (posible cambio de DOM o iframe).")

    logger.info("Campos encontrados, escribiendo credenciales...")
    # Escribir y disparar eventos
    driver.execute_script("arguments[0].focus();", usr)
    usr.clear()
    usr.send_keys(USERNAME)
    driver.execute_script("arguments[0].dispatchEvent(new Event('input',{bubbles:true}))", usr)

    driver.execute_script("arguments[0].focus();", pwd)
    pwd.clear()
    pwd.send_keys(PASSWORD)
    driver.execute_script("arguments[0].dispatchEvent(new Event('input',{bubbles:true}))", pwd)
    driver.execute_script("arguments[0].dispatchEvent(new Event('change',{bubbles:true}))", pwd)
    logger.info("Credenciales escritas, buscando botón de login...")

    # Intento de envío
    btn, _ = find_first(driver, btn_selectors)
    if btn:
        logger.info("Botón de login encontrado, haciendo click...")
        if not robust_click(driver, btn):
            logger.info("Click falló, usando ENTER...")
            pwd.send_keys(Keys.ENTER)
    else:
        logger.info("Botón de login no encontrado, intentando submit del form...")
        # Si no hay botón visible, intenta submit del form
        form = None
        try:
            form = driver.find_element(By.CSS_SELECTOR, "form#fm1")
        except:
            try:
                form = driver.find_element(By.CSS_SELECTOR, "form")
            except:
                pass
        if form:
            logger.info("Form encontrado, haciendo submit...")
            driver.execute_script("arguments[0].submit();", form)
        else:
            logger.info("Form no encontrado, usando ENTER...")
            pwd.send_keys(Keys.ENTER)

    # Espera redirección
    logger.info("Esperando redirección a homepage...")
    try:
        wait.until(EC.url_contains("homepage.html"))
        logger.info("Redirección exitosa")
    except Exception as e:
        logger.info("Timeout en primera espera de redirección, reintentando...")
        # Reintento: puede haberse quedado esperando
        if btn:
            robust_click(driver, btn)
            try:
                wait.until(EC.url_contains("homepage.html"))
                logger.info("Redirección exitosa en reintento")
            except Exception as e2:
                logger.error("Timeout en reintento de redirección: %s", e2)
                pass

    # Si no llegamos, captura evidencia y aborta
    if "homepage.html" not in driver.current_url:
        ts = time.strftime("%Y%m%d_%H%M%S")
        screenshot = os.path.join(ruta_descarga, f"login_error_{ts}.png")
        htmldump = os.path.join(ruta_descarga, f"login_error_{ts}.html")
        driver.save_screenshot(screenshot)
        with open(htmldump, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        raise RuntimeError(f"No se pudo completar el login. Evidencia:\n{screenshot}\n{htmldump}")

    logger.info("Login realizado.")
    driver.switch_to.default_content()


def main():
    # === CONFIGURACIÓN ===
    # En Linux/Airflow, define GDE_DOWNLOAD_DIR=/opt/airflow/data/gde, por ejemplo
    ruta_descarga = os.getenv(
        "GDE_DOWNLOAD_DIR",
        r"C:\Users\Usuario\Documents\GitHub\scraping"  # por defecto en Windows local
    )
    Path(ruta_descarga).mkdir(parents=True, exist_ok=True)
    logger.info(f"Ruta de descarga: {ruta_descarga}")

    # === CREAR DRIVER CON IDIOMA ES-ES ===
    try:
        driver = get_driver(ruta_descarga)
    except Exception as e:
        logger.error("Error al crear ChromeDriver: %s", e, exc_info=True)
        raise

    wait = WebDriverWait(driver, 20)

    try:
        # === LOGIN (robusto) ===
        logger.info("Iniciando proceso de login...")
        do_login_robusto(driver, wait, ruta_descarga)
        logger.info("Login completado, continuando con el flujo...")

        # === ESPERAR Y CAMBIAR AL IFRAME PRINCIPAL (filtros) ===
        logger.info("Esperando a que cargue el iframe con filtros...")
        iframe_encontrado = False
        intentosf = 0

        while not iframe_encontrado and intentosf < max_intentosf:
            try:
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                for frame in iframes:
                    driver.switch_to.frame(frame)
                    # Señal de que es el iframe de filtros
                    if driver.execute_script("return !!document.querySelector('.ows_filter_title');"):
                        logger.info("Iframe principal encontrado.")
                        iframe_encontrado = True
                        break
                    driver.switch_to.default_content()

                if not iframe_encontrado:
                    intentosf += 1
                    logger.info(f"Intento {intentosf}/{max_intentosf} - Esperando carga del iframe...")
                    sleep(2)

            except Exception as e:
                logger.info(f"Error en intento {intentosf}: {e}")
                driver.switch_to.default_content()
                intentosf += 1
                sleep(2)

        if not iframe_encontrado:
            logger.info("No se encontró el iframe después de esperar. Continuando sin iframe...")
        else:
            logger.info("Iframe cargado correctamente.")

        # === ABRIR PANEL DE FILTROS ===
        script_check = """
            const elements = document.getElementsByClassName("v-icon-o-filter");
            return elements.length;
        """
        num_elementos = driver.execute_script(script_check)
        logger.info(f"Elementos 'v-icon-o-filter' encontrados: {num_elementos}")

        if num_elementos >= 5:
            driver.execute_script('document.getElementsByClassName("v-icon-o-filter")[4].click();')
            logger.info("Panel de filtros abierto (índice 4).")
        elif num_elementos > 0:
            index_to_use = num_elementos - 1
            driver.execute_script(f'document.getElementsByClassName("v-icon-o-filter")[{index_to_use}].click();')
            logger.info(f"Panel de filtros abierto (índice {index_to_use}).")
        else:
            # Buscar en todos los iframes
            logger.info("Elemento no encontrado en contexto actual, buscando en iframes...")
            driver.switch_to.default_content()
            frames = driver.find_elements(By.TAG_NAME, "iframe")
            logger.info(f"Total de iframes: {len(frames)}")

            for idx, frame in enumerate(frames):
                driver.switch_to.frame(frame)
                num_elementos = driver.execute_script(script_check)
                logger.info(f"Iframe {idx}: {num_elementos} elementos")

                if num_elementos > 0:
                    index_to_use = min(4, num_elementos - 1)
                    driver.execute_script(
                        f'document.getElementsByClassName("v-icon-o-filter")[{index_to_use}].click();'
                    )
                    logger.info(f"Panel de filtros abierto en iframe {idx} (índice {index_to_use}).")
                    break
                driver.switch_to.default_content()

        sleep(1)

        # Click clear
        driver.find_element(By.XPATH, '//*[@id="allTask_tab"]/form/div[2]/div/div/div[2]/button[2]').click()
        logger.info("Se limpió el filtro")
        sleep(1)

        try:
            # Abrir desplegable
            logger.info("Abriendo desplegable...")
            driver.find_element(By.CSS_SELECTOR, "#all_taskType .el-select__caret").click()
            sleep(1)

            # Seleccionar por XPath con title
            for opcion in opciones_a_seleccionar:
                xpath = f"//li[contains(@class, 'el-select-dropdown__item') and @title='{opcion}']"
                wait.until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
                logger.info(f"{opcion}")
                sleep(0.3)

            logger.info("Completado")
        except Exception as e:
            logger.info(f"Error abriendo/seleccionando en el desplegable: {e}")

        # === APLICAR FILTROS SEGÚN VARIABLE ===
        if var == 1:
            fecha_desde, fecha_hasta = calcular_rango_fechas()
            logger.info("Usando rango de fechas: DESDE %s HASTA %s", fecha_desde, fecha_hasta)
            # Asegúrate de estar en el iframe de filtros
            switch_to_frame_with(driver, ".ows_filter_title")

            def click_y_setear_fecha_y_hora(container_xpath: str, fecha: str, hora: str):
                # 1) Intenta abrir el modo "intervalo personalizado" con el botón +
                try:
                    plus = driver.find_element(
                        By.CSS_SELECTOR,
                        ".ows_datetime_interval_customer_text.el-icon-circle-plus"
                    )
                    if plus.is_displayed():
                        robust_click(driver, plus)
                        time.sleep(1)
                except Exception:
                    # si no existe, seguimos sin problema
                    pass

                # 2) Click en el contenedor (DESDE o HASTA)
                cont = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, container_xpath))
                )
                robust_click(driver, cont)
                time.sleep(1)

                # 3) Buscar input "Seleccionar fecha" y setear valor
                target = None
                inputs_fecha = driver.find_elements(
                    By.CSS_SELECTOR,
                    'input.el-input__inner[placeholder="Seleccionar fecha"]'
                )
                for el in inputs_fecha:
                    if el.is_displayed() and el.is_enabled():
                        target = el  # suele ser el del popover activo

                # fallback: buscar dentro del propio contenedor
                if target is None:
                    try:
                        target = cont.find_element(
                            By.CSS_SELECTOR,
                            'input.el-input__inner[placeholder="Seleccionar fecha"]'
                        )
                    except Exception:
                        pass

                if target is None:
                    raise RuntimeError("No se encontró el input 'Seleccionar fecha' después de abrir el selector.")

                # Escribir la FECHA de forma robusta
                try:
                    target.click()
                    time.sleep(1)
                    target.send_keys(Keys.CONTROL, 'a')
                    target.send_keys(Keys.DELETE)
                    target.send_keys(fecha)
                    # Disparar eventos para que el framework detecte el cambio
                    driver.execute_script(
                        "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
                        "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                        target
                    )
                    target.send_keys(Keys.ENTER)  # confirma/cierra el popover de fecha
                    time.sleep(1)
                except Exception as e:
                    raise RuntimeError(f"No se pudo escribir la fecha '{fecha}': {e}")

                # 4) Buscar input "Seleccionar hora" y setear valor
                time_input = None
                inputs_hora = driver.find_elements(
                    By.CSS_SELECTOR,
                    'input.el-input__inner[placeholder="Seleccionar hora"]'
                )
                for el in inputs_hora:
                    if el.is_displayed() and el.is_enabled():
                        time_input = el
                        break

                if time_input is None:
                    raise RuntimeError("No se encontró el input 'Seleccionar hora'.")

                try:
                    time_input.click()
                    time.sleep(0.5)
                    time_input.send_keys(Keys.CONTROL, 'a')
                    time_input.send_keys(Keys.DELETE)
                    time_input.send_keys(hora)
                    driver.execute_script(
                        "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
                        "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                        time_input
                    )
                    time_input.send_keys(Keys.ENTER)
                    time.sleep(1)
                except Exception as e:
                    raise RuntimeError(f"No se pudo escribir la hora '{hora}': {e}")

            # === DESDE ===
            click_y_setear_fecha_y_hora(
                '//*[@id="createtimeRow"]/div[2]/div[2]/div/div[2]/div/div[1]',
                fecha_desde,
                hora_desde
            )
            logger.info(f"Fecha DESDE aplicada: {fecha_desde} {hora_desde}")
            time.sleep(1)

            # === HASTA ===
            click_y_setear_fecha_y_hora(
                '//*[@id="createtimeRow"]/div[2]/div[2]/div/div[2]/div/div[2]',
                fecha_hasta,
                hora_hasta
            )
            logger.info(f"Fecha HASTA aplicada: {fecha_hasta} {hora_hasta}")
            time.sleep(1)

        elif var == 2:
            driver.find_element(
                By.XPATH,
                '//*[@id="createtimeRow"]/div[2]/div[2]/div/div[1]/label[3]'
            ).click()
            logger.info("Se asignó 'Último mes'")
            time.sleep(1)
        else:
            logger.info("Variable no válida. Usa 1 o 2.")

        # Aplica filtro haciendo click en el botón "Filtrar"
        try:
            filter_btn = driver.find_element(
                By.XPATH,
                "//span[contains(@class,'sdm_splitbutton_text') and normalize-space()='Filtrar']"
            )
            if not robust_click(driver, filter_btn):
                filter_btn.click()
            logger.info("Filtro aplicado (botón 'Filtrar')")
        except Exception as e:
            logger.info(f"No se pudo hacer click en 'Filtrar': {e}")

        sleep(2)

        # Click en exportar
        driver.find_element(By.CSS_SELECTOR, "#test > .sdm_splitbutton_text").click()
        logger.info("Exportar")
        sleep(1)

        # === VOLVER AL CONTENIDO PRINCIPAL ===
        driver.switch_to.default_content()

        # Cerrar modal/panel
        driver.find_element(By.CSS_SELECTOR, ".el-icon-close:nth-child(2)").click()
        logger.info("Cerrar modal/panel")
        sleep(1)

        # Click en sexto item del sidebar (icono)
        driver.find_element(By.CSS_SELECTOR, ".el-row:nth-child(6) > .side-item-icon").click()
        logger.info("Click en sidebar item 6")
        sleep(1)

        # Click en level-1
        driver.find_element(By.CSS_SELECTOR, ".level-1").click()
        logger.info("Click en level-1")
        sleep(1)

        # === CAMBIAR AL SEGUNDO IFRAME ===
        driver.switch_to.frame(1)
        logger.info("Cambiado a iframe index=1")
        sleep(1)

        # === CAPTURAR ID DE LA PRIMERA FILA (NUESTRO JOB) ===
        try:
            primera_fila = driver.find_element(
                By.XPATH,
                '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]'
            )
            job_id = primera_fila.find_element(
                By.XPATH,
                './td[2]/div/span'
            ).text.strip()
            logger.info(f"ID de la tarea creada (fila 1): {job_id}")
        except Exception as e:
            logger.info(f"No se pudo obtener el ID de la primera fila: {e}")
            job_id = None

        # Función auxiliar para encontrar la fila actual de ese ID
        def encontrar_fila_por_id():
            """
            Busca en todas las filas del grid la que tenga nuestro job_id en la columna 2.
            Devuelve (fila, índice_fila) o (None, None) si no la encuentra.
            """
            if not job_id:
                return None, None

            filas = driver.find_elements(
                By.XPATH,
                '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr'
            )
            for idx, fila in enumerate(filas, start=1):
                try:
                    id_text = fila.find_element(
                        By.XPATH,
                        './td[2]/div/span'
                    ).text.strip()
                    if id_text == job_id:
                        return fila, idx
                except Exception:
                    continue
            return None, None

        # === BUCLE DE REFRESH CON VERIFICACIÓN DE ESTADO ===
        logger.info("Iniciando proceso de carga...")
        intentoc = 0

        while intentoc < max_intentosC:
            try:
                # Click en botón Refresh
                driver.find_element(By.CSS_SELECTOR, "span.button_icon.btnIcon[style*='refresh']").click()
                logger.info(f"Refresh {intentoc + 1}/{max_intentosC}")
                sleep(3)  # Esperar que se actualice la tabla

                fila_job, row_index = encontrar_fila_por_id()
                if fila_job is None:
                    logger.info("No se encontró la fila con nuestro ID. Reintentando...")
                    intentoc += 1
                    sleep(10)
                    continue

                # Obtener el estado actual desde la fila encontrada
                estado = fila_job.find_element(
                    By.XPATH,
                    './td[3]/div/span'
                ).text.strip()
                logger.info(f"Estado (fila {row_index}, ID {job_id}): {estado}")

                # Verificar el estado
                if estado == "Succeed":
                    logger.info("Carga completada exitosamente!")
                    break
                elif estado == "Failed":
                    logger.info("Carga fallida!")
                    break
                elif estado == "Aborted":
                    logger.info("Proceso abortado!")
                    break
                elif estado == "Waiting":
                    logger.info("Proceso Waiting!")
                    break
                elif estado == "Concurrent Waiting":
                    logger.info("Proceso Concurrent Waiting!")
                    break
                elif estado == "Running":
                    logger.info("Aún procesando... esperando 10 segundos")
                    sleep(10)
                    intentoc += 1
                else:
                    logger.info(f"Estado: '{estado}' - Continuando...")
                    sleep(10)
                    intentoc += 1

            except Exception as e:
                logger.info(f"Error al verificar estado: {e}")
                sleep(10)
                intentoc += 1

        if intentoc >= max_intentosC:
            logger.info("Tiempo máximo de espera alcanzado")

        # === DESCARGAR ARCHIVO ===
        logger.info("Iniciando descarga...")

        # Contar archivos antes de descargar
        archivos_antes = set(os.listdir(ruta_descarga))

        # Buscar de nuevo la fila de nuestro ID (por si se movió)
        fila_job, row_index = encontrar_fila_por_id()

        if fila_job is not None:
            try:
                boton_descarga = fila_job.find_element(
                    By.XPATH,
                    './td[11]/div/div[3]'
                )
                boton_descarga.click()
                logger.info(f"Click en botón de descarga (fila {row_index}, ID {job_id})")
            except Exception as e:
                logger.info(f"No se pudo hacer click en el botón de descarga de nuestra fila: {e}")
                # Fallback: intentamos la primera fila como antes
                driver.find_element(
                    By.XPATH,
                    '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]/td[11]/div/div[3]'
                ).click()
                logger.info("Click en botón de descarga (fallback: primera fila)")
        else:
            logger.info("No se encontró la fila de nuestro ID al descargar. Usando primera fila como fallback.")
            driver.find_element(
                By.XPATH,
                '//*[@id="testGrid"]/div[1]/div[3]/table/tbody/tr[1]/td[11]/div/div[3]'
            ).click()
            logger.info("Click en botón de descarga (fallback: primera fila)")

        # Esperar que aparezca el archivo
        logger.info("Esperando descarga...")
        timeout = 60  # Esperar máximo 60 segundos
        inicio = time.time()

        while time.time() - inicio < timeout:
            archivos_ahora = set(os.listdir(ruta_descarga))
            archivos_nuevos = archivos_ahora - archivos_antes

            # Filtrar archivos temporales de Chrome
            archivos_completos = [f for f in archivos_nuevos if not f.endswith(".crdownload")]

            if archivos_completos:
                archivo_descargado = archivos_completos[0]
                logger.info(f"Descarga completada: {archivo_descargado}")
                logger.info(f"Ruta: {os.path.join(ruta_descarga, archivo_descargado)}")
                break

            sleep(2)
        else:
            logger.info("Timeout: La descarga tardó más de 60 segundos")

    except Exception:
        logger.error("Error durante el proceso:", exc_info=True)
    finally:
        # No usar input() en Airflow - se cierra automáticamente
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()