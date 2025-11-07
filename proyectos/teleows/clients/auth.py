"""
AuthManager: login y validación de sesiones en el portal Integratel.

Este módulo es consumido por los workflows (`teleows.workflows.gde` y
`teleows.workflows.dynamic_checklist`). Mantiene todo el código relacionado con
autenticación en un único lugar (paquete `teleows.clients`).
"""

import logging
import time

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)


class AuthManager:
    """
    Gestiona el proceso de login en el portal.

    Usado directamente por los workflows. Mantener esta clase liviana permite
    reutilizarla en futuras automatizaciones que compartan el mismo portal.
    """

    LOGIN_URL = (
        "https://30c8-mx.teleows.com/dspcas/login?service="
        "https://30c8-mx.teleows.com/portal/web/rest/sso/"
        "index%3Fori_url%3Dhttps%253A%252F%252F30c8-mx.teleows.com%"
        "252Fportal-web%252Fportal%252Fhomepage.html"
    )
    SUCCESS_URL = "homepage.html"

    def __init__(self, driver, wait_timeout: int = 20) -> None:
        self.driver = driver
        self.wait = WebDriverWait(driver, wait_timeout)

    def login(self, username: str, password: str) -> bool:
        """Ejecuta el login y valida la redirección al homepage."""
        try:
            logger.info("🔐 Iniciando proceso de login...")
            self.driver.get(self.LOGIN_URL)

            username_field = self.wait.until(
                EC.visibility_of_element_located((By.ID, "username"))
            )
            username_field.send_keys(username)

            password_field = self.wait.until(
                EC.visibility_of_element_located((By.ID, "password"))
            )
            password_field.send_keys(password)

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

            if self.verify_login_success():
                logger.info("✅ Login realizado exitosamente.")
                time.sleep(5)
                return True

            lock_message = self._extract_portal_message()
            if lock_message:
                logger.error("🚫 Login rechazado por el portal: %s", lock_message)
                raise RuntimeError(lock_message)

            message = "Login falló: no se alcanzó la URL de destino."
            logger.error("❌ %s", message)
            raise RuntimeError(message)
        except Exception as exc:  # pragma: no cover - captura defensiva
            message = f"Error durante el login: {exc}"
            logger.error("❌ %s", message, exc_info=True)
            raise RuntimeError(message) from exc

    def verify_login_success(self) -> bool:
        """Comprueba que la URL actual corresponde al homepage esperado."""
        try:
            self.wait.until(EC.url_contains(self.SUCCESS_URL))
            current = self.driver.current_url
            if "dspcas/login" in current:
                return False
            return self.SUCCESS_URL in current
        except Exception:
            return False

    def _extract_portal_message(self) -> str | None:
        """Busca mensajes visibles en el DOM (bloqueos, credenciales inválidas, etc.)."""
        try:
            body_text = self.driver.execute_script("return document.body.innerText || ''")
        except Exception:
            body_text = ""

        normalized = body_text.strip()
        if not normalized:
            return None

        if all(keyword in normalized.lower() for keyword in ("user", "locked")):
            return normalized.splitlines()[0]

        return None

    def is_logged_in(self) -> bool:
        """Consulta rápida para saber si la sesión está activa."""
        try:
            return self.SUCCESS_URL in self.driver.current_url
        except Exception:
            return False
