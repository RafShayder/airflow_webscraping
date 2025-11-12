"""
BrowserManager: configurador de sesiones Chrome/Chromium para los scrapers.

Copiado de ``teleows.core.browser_manager`` y alojado ahora dentro de
``teleows.clients`` para mantener juntos los componentes que envuelven Selenium.
"""

import os
import shutil
from pathlib import Path
from typing import Iterable, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait


class BrowserManager:
    """Crea drivers de Chrome con configuración consistente (descargas, proxy, headless)."""

    def __init__(
        self,
        download_path: Optional[str] = None,
        wait_timeout: int = 20,
        headless: bool = False,
        extra_args: Optional[Iterable[str]] = None,
    ) -> None:
        self.download_path = download_path
        self.wait_timeout = wait_timeout
        self.headless = headless
        self.extra_args = list(extra_args or [])
        self.driver = None
        self.wait = None

    def setup_chrome_options(self) -> Options:
        """Prepara la instancia de ``Options`` respetando la configuración del entorno."""
        options = Options()
        options.add_argument("--start-maximized")

        if self.headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")

        proxy = self.proxy or os.getenv("PROXY")
        if proxy:
            proxy_arg = proxy if "://" in proxy else f"http://{proxy}"
            options.add_argument(f"--proxy-server={proxy_arg}")

        for arg in self.extra_args:
            options.add_argument(arg)

        if self.download_path:
            abs_download_path = str(Path(self.download_path).resolve())
            Path(abs_download_path).mkdir(parents=True, exist_ok=True)
            print(f"Configurando descargas en: {abs_download_path}")
            prefs = {
                "download.default_directory": abs_download_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "profile.default_content_settings.popups": 0,
                "profile.default_content_setting_values.automatic_downloads": 1,
            }
            options.add_experimental_option("prefs", prefs)
        else:
            print("No se especificó ruta de descarga - usando carpeta por defecto del sistema")

        return options

    def create_driver(self):
        """Devuelve ``(driver, wait)`` configurados según las opciones indicadas."""
        if self.driver is not None:
            return self.driver, self.wait

        options = self.setup_chrome_options()

        if os.path.exists("/usr/bin/chromium"):
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--remote-debugging-port=9222")
            options.add_argument("--disable-setuid-sandbox")
            options.add_argument("--disable-extensions")
            options.binary_location = "/usr/bin/chromium"
            print("Configuración Docker/Chromium aplicada")

        chromedriver_path = None
        if os.path.exists("/usr/bin/chromedriver"):
            chromedriver_path = "/usr/bin/chromedriver"
            print(f"Docker detectado - usando ChromeDriver del sistema: {chromedriver_path}")
        elif os.path.exists("/usr/local/bin/chromedriver") and os.path.islink("/usr/local/bin/chromedriver"):
            real_path = os.path.realpath("/usr/local/bin/chromedriver")
            if os.path.exists(real_path):
                chromedriver_path = real_path
                print(f"🔗 Usando archivo real del symlink: {chromedriver_path}")
            else:
                chromedriver_path = "/usr/local/bin/chromedriver"
                print(f"✓ ChromeDriver encontrado en local: {chromedriver_path}")
        elif shutil.which("chromedriver"):
            chromedriver_path = shutil.which("chromedriver")
            print(f"✓ ChromeDriver encontrado en PATH: {chromedriver_path}")

        service = Service(chromedriver_path) if chromedriver_path else Service()

        self.driver = webdriver.Chrome(options=options, service=service)
        self.wait = WebDriverWait(self.driver, self.wait_timeout)
        return self.driver, self.wait

    def close_driver(self) -> None:
        if self.driver:
            self.driver.quit()
            self.driver = None
            self.wait = None

    def __enter__(self):
        return self.create_driver()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_driver()
