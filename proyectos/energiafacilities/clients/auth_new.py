import logging
from dataclasses import dataclass
from typing import Optional

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoginSelectors:
    username: tuple
    password: tuple
    submit: tuple


class AuthManager:
    """
    Gestor genérico de autenticación para portales web con Selenium.

    - Mantiene la sesión viva usando el mismo driver
    - Reutilizable para múltiples portales
    """

    def __init__(
        self,
        driver,
        login_url: str,
        success_url_contains: str,
        selectors: LoginSelectors,
        timeout: int = 60,
    ) -> None:
        self.driver = driver
        self.login_url = login_url
        self.success_url_contains = success_url_contains
        self.selectors = selectors
        self.wait = WebDriverWait(driver, timeout)

    # ============================
    # Public API
    # ============================

    def login(self, username: str, password: str) -> bool:
        """
        Ejecuta el login solo si no existe una sesión activa.
        """
        if self.is_logged_in():
            logger.info("Sesión ya activa, se omite login")
            return True

        logger.info("Iniciando login en %s", self.login_url)
        self.driver.get(self.login_url)

        try:
            self._fill_credentials(username, password)
            self._submit_login()

            if not self._wait_for_success():
                message = self._extract_portal_message()
                raise RuntimeError(message or "Login fallido: no se detectó sesión activa")

            logger.info("Login exitoso")
            return True

        except Exception as exc:
            logger.error("Error durante login: %s", exc, exc_info=True)
            raise

    def is_logged_in(self) -> bool:
        """
        Verificación rápida de sesión activa.
        """
        try:
            return self.success_url_contains in self.driver.current_url
        except Exception:
            return False

    # ============================
    # Internal helpers
    # ============================

    def _fill_credentials(self, username: str, password: str) -> None:
        self.wait.until(EC.visibility_of_element_located(self.selectors.username)).send_keys(username)
        self.wait.until(EC.visibility_of_element_located(self.selectors.password)).send_keys(password)

    def _submit_login(self) -> None:
        button = self.wait.until(EC.element_to_be_clickable(self.selectors.submit))
        button.click()

        # Esperar navegación real
        try:
            self.wait.until(EC.staleness_of(button))
        except TimeoutException:
            logger.debug("Botón no se invalidó, posible SPA")

    def _wait_for_success(self) -> bool:
        try:     
            self.wait.until(EC.url_contains(self.success_url_contains))
            return True
        except TimeoutException:
            logger.warning("No se alcanzó URL de éxito")
            return False

    def _extract_portal_message(self) -> Optional[str]:
        """
        Extrae mensajes visibles del portal (errores, bloqueos, etc.)
        """
        try:
            text = self.driver.execute_script(
                "return document.body?.innerText || ''"
            ).strip()
        except Exception:
            return None

        if not text:
            return None

        for keyword in ("locked", "invalid", "error"):
            if keyword in text.lower():
                return text.splitlines()[0]

        return None
