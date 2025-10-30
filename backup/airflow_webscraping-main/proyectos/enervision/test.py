from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time


def pruebawebscraping():
    # Configurar opciones de Chrome
    options = Options()
    options.add_argument("--headless")           # Modo sin interfaz gráfica
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Iniciar el driver (ChromeDriver ya está en /usr/local/bin/chromedriver)
    service = Service("/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    #driver = webdriver.Chrome(options=options)
    print(" ChromeDriver iniciado correctamente.")

    # Ir a una página de prueba
    driver.get("https://www.google.com")
    print(" Título de la página:", driver.title)

    # Esperar 2 segundos
    time.sleep(2)

    # Cerrar el navegador
    driver.quit()
    print(" Finalizado correctamente.")


