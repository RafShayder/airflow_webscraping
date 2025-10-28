"""
Módulo de autenticación para el portal de Integratel.
Maneja el proceso de login y verificación de autenticación.
"""

import logging
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


logger = logging.getLogger(__name__)


class AuthManager:
    """Maneja la autenticación en el portal de Integratel."""
    
    # URL del portal de login
    LOGIN_URL = (
        "https://30c8-mx.teleows.com/dspcas/login?service="
        "https://30c8-mx.teleows.com/portal/web/rest/sso/"
        "index%3Fori_url%3Dhttps%253A%252F%252F30c8-mx.teleows.com%"
        "252Fportal-web%252Fportal%252Fhomepage.html"
    )
    
    # URL de éxito después del login
    SUCCESS_URL = "homepage.html"
    
    def __init__(self, driver, wait_timeout=20):
        """
        Inicializa el AuthManager.
        
        Args:
            driver: Instancia de WebDriver
            wait_timeout: Tiempo de espera para elementos (segundos)
        """
        self.driver = driver
        self.wait = WebDriverWait(driver, wait_timeout)
    
    def login(self, username, password):
        """
        Realiza el proceso de login en el portal.
        
        Args:
            username: Nombre de usuario
            password: Contraseña
            
        Returns:
            bool: True si el login fue exitoso
            
        Raises:
            RuntimeError: Si no se puede completar el login
        """
        try:
            logger.info("🔐 Iniciando proceso de login...")
            
            # Abre la pantalla de autenticación del portal.
            self.driver.get(self.LOGIN_URL)
            
            # Completa credenciales en los campos visibles.
            username_field = self.wait.until(
                EC.visibility_of_element_located((By.ID, "username"))
            )
            username_field.send_keys(username)
            
            password_field = self.wait.until(
                EC.visibility_of_element_located((By.ID, "password"))
            )
            password_field.send_keys(password)
            
            # Ejecuta el envío del formulario (doble clic según comportamiento detectado).
            login_button = self.wait.until(
                EC.element_to_be_clickable((By.ID, "loginButton"))
            )
            login_button.click()
            try:
                self.wait.until(EC.staleness_of(login_button))
            except TimeoutException:
                logger.warning("🔁 Botón de login sigue activo, intentando segundo clic.")
                second_button = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "loginButton"))
                )
                second_button.click()
            
            # Valida que la navegación redirige correctamente al homepage.
            if self.verify_login_success():
                logger.info("✅ Login realizado exitosamente.")
                time.sleep(5)
                return True

            # Si no hubo redirección, intentamos extraer el mensaje mostrado por el portal.
            lock_message = self._extract_portal_message()
            if lock_message:
                logger.error("🚫 Login rechazado por el portal: %s", lock_message)
                raise RuntimeError(lock_message)
            
            message = "Login falló: no se alcanzó la URL de destino."
            logger.error("❌ %s", message)
            raise RuntimeError(message)
                
        except Exception as exc:
            message = f"Error durante el login: {exc}"
            logger.error("❌ %s", message, exc_info=True)
            raise RuntimeError(message) from exc
    
    def verify_login_success(self):
        """
        Verifica que el login fue exitoso revisando la URL.
        
        Returns:
            bool: True si el login fue exitoso, False en caso contrario
        """
        try:
            self.wait.until(EC.url_contains(self.SUCCESS_URL))
            current = self.driver.current_url
            # Aseguramos que ya no seguimos en la pantalla de login.
            if "dspcas/login" in current:
                return False
            return self.SUCCESS_URL in current
        except Exception:
            return False

    def _extract_portal_message(self):
        """
        Inspecciona la página actual en busca de mensajes relevantes del portal (ej. bloqueo de usuario).
        
        Returns:
            str | None: Mensaje detectado o None si no se encontró.
        """
        try:
            body_text = self.driver.execute_script("return document.body.innerText || ''")
        except Exception:
            body_text = ""

        normalized = body_text.strip()
        if not normalized:
            return None

        keywords = [
            "user",
            "locked",
            "Please contact the administrator",
            "wait for the system to unlock automatically",
        ]
        if all(keyword.lower() in normalized.lower() for keyword in ("user", "locked")):
            # Resumimos el mensaje a la primera línea para logs más limpios.
            first_line = normalized.splitlines()[0]
            return first_line

        return None
    
    def is_logged_in(self):
        """
        Verifica si el usuario está actualmente logueado.
        
        Returns:
            bool: True si está logueado, False en caso contrario
        """
        try:
            return self.SUCCESS_URL in self.driver.current_url
        except Exception:
            return False
