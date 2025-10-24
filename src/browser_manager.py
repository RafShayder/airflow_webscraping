"""
Módulo para manejar la configuración y creación del navegador Chrome.
Centraliza la configuración de Chrome para reutilización en múltiples scripts.
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from pathlib import Path


class BrowserManager:
    """Maneja la configuración y creación del navegador Chrome."""
    
    def __init__(self, download_path=None, wait_timeout=20, headless=False, extra_args=None):
        """
        Inicializa el BrowserManager.
        
        Args:
            download_path (str): Ruta donde se descargarán los archivos
            wait_timeout (int): Timeout para WebDriverWait en segundos
            headless (bool): Indica si el navegador debe ejecutarse en modo headless
            extra_args (list[str]): Argumentos adicionales para Chrome
        """
        self.download_path = download_path
        self.wait_timeout = wait_timeout
        self.headless = headless
        self.extra_args = extra_args or []
        self.driver = None
        self.wait = None
    
    def setup_chrome_options(self):
        """
        Configura las opciones de Chrome.
        
        Returns:
            Options: Objeto con las opciones configuradas
        """
        options = Options()
        options.add_argument("--start-maximized")
        
        if self.headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
        
        for arg in self.extra_args:
            options.add_argument(arg)
        
        # Configurar preferencias de descarga si se especifica una ruta
        if self.download_path:
            # Convertir a ruta absoluta y crear directorio si no existe
            abs_download_path = str(Path(self.download_path).resolve())
            Path(abs_download_path).mkdir(parents=True, exist_ok=True)
            
            print(f"📁 Configurando descargas en: {abs_download_path}")
            
            prefs = {
                "download.default_directory": abs_download_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "profile.default_content_settings.popups": 0,
                "profile.default_content_setting_values.automatic_downloads": 1
            }
            options.add_experimental_option("prefs", prefs)
        else:
            print("⚠️ No se especificó ruta de descarga - usando carpeta por defecto del sistema")
        
        return options
    
    def create_driver(self):
        """
        Crea y configura el driver de Chrome.
        
        Returns:
            tuple: (driver, wait) - Instancia del driver y WebDriverWait
        """
        if self.driver is not None:
            return self.driver, self.wait
        
        # Configurar opciones
        options = self.setup_chrome_options()
        
        # Crear driver
        self.driver = webdriver.Chrome(options=options, service=Service())
        self.wait = WebDriverWait(self.driver, self.wait_timeout)
        
        return self.driver, self.wait
    
    def close_driver(self):
        """Cierra el driver si está abierto."""
        if self.driver:
            self.driver.quit()
            self.driver = None
            self.wait = None
    
    def __enter__(self):
        """Context manager entry."""
        return self.create_driver()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_driver()
