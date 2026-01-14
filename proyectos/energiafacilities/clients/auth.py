"""
AuthManager: gestor unificado de autenticación para portales web con Selenium.

Soporta dos modos de uso:
1. Modo legacy (Integratel): URLs y selectores hardcodeados para compatibilidad
2. Modo genérico: URLs y selectores configurables para cualquier portal

Este módulo es consumido por los workflows (GDE, Dynamic Checklist, NetEco, etc.).
"""

import logging
import time
from dataclasses import dataclass
from typing import Optional

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoginSelectors:
    """Selectores para campos de login (username, password, submit button)."""
    username: tuple
    password: tuple
    submit: tuple


class AuthManager:
    """
    Gestor unificado de autenticación para portales web con Selenium.

    Modo Legacy (Integratel):
        auth = AuthManager(driver)  # Usa URLs y selectores por defecto

    Modo Genérico:
        selectors = LoginSelectors(
            username=(By.ID, "username"),
            password=(By.ID, "value"),
            submit=(By.ID, "submitDataverify"),
        )
        auth = AuthManager(
            driver=driver,
            login_url="https://example.com/login",
            success_url_contains="dashboard",
            selectors=selectors,
        )
    """

    # URLs por defecto para Integratel (modo legacy)
    DEFAULT_LOGIN_URL = (
        "https://30c8-mx.teleows.com/dspcas/login?service="
        "https://30c8-mx.teleows.com/portal/web/rest/sso/"
        "index%3Fori_url%3Dhttps%253A%252F%252F30c8-mx.teleows.com%"
        "252Fportal-web%252Fportal%252Fhomepage.html"
    )
    DEFAULT_SUCCESS_URL = "homepage.html"

    # Selectores por defecto para Integratel (modo legacy)
    DEFAULT_SELECTORS = LoginSelectors(
        username=(By.ID, "username"),
        password=(By.ID, "password"),
        submit=(By.ID, "loginButton"),
    )

    def __init__(
        self,
        driver,
        login_url: Optional[str] = None,
        success_url_contains: Optional[str] = None,
        selectors: Optional[LoginSelectors] = None,
        wait_timeout: int = 60,
        legacy_mode: bool = False,
    ) -> None:
        """
        Inicializa AuthManager.

        Args:
            driver: Instancia de WebDriver de Selenium
            login_url: URL de login (opcional, usa Integratel por defecto si no se proporciona)
            success_url_contains: Fragmento de URL que indica login exitoso (opcional)
            selectors: Selectores personalizados (opcional, usa Integratel por defecto)
            wait_timeout: Timeout para esperas de Selenium
            legacy_mode: Si True, fuerza modo legacy incluso con parámetros personalizados
        """
        self.driver = driver
        self.wait = WebDriverWait(driver, wait_timeout)

        # Determinar modo de operación
        if legacy_mode or (login_url is None and success_url_contains is None and selectors is None):
            # Modo legacy: usar valores por defecto de Integratel
            self.login_url = self.DEFAULT_LOGIN_URL
            self.success_url_contains = self.DEFAULT_SUCCESS_URL
            self.selectors = self.DEFAULT_SELECTORS
            self._is_legacy = True
            logger.debug("AuthManager inicializado en modo legacy (Integratel)")
        else:
            # Modo genérico: usar valores proporcionados
            if login_url is None or success_url_contains is None or selectors is None:
                logger.error("En modo genérico, deben proporcionarse login_url, success_url_contains y selectors")
                raise ValueError(
                    "En modo genérico, deben proporcionarse login_url, success_url_contains y selectors"
                )
            self.login_url = login_url
            self.success_url_contains = success_url_contains
            self.selectors = selectors
            self._is_legacy = False
            logger.debug("AuthManager inicializado en modo genérico")

    def login(self, username: str, password: str) -> bool:
        """
        Ejecuta el login y valida la redirección.

        Args:
            username: Nombre de usuario
            password: Contraseña

        Returns:
            True si el login fue exitoso

        Raises:
            RuntimeError: Si el login falla
        """
        # Verificar si ya hay sesión activa (solo en modo genérico)
        if not self._is_legacy:
            if self.is_logged_in():
                logger.info("Sesión ya activa, se omite login")
                return True

        try:
            logger.debug("Iniciando proceso de login")
            self.driver.get(self.login_url)

            # Llenar credenciales
            self._fill_credentials(username, password)

            # Enviar formulario
            if self._is_legacy:
                self._submit_login_legacy()
            else:
                self._submit_login()

            # Verificar éxito
            if self._is_legacy:
                if self.verify_login_success():
                    logger.info("Login realizado exitosamente")
                    time.sleep(5)  # Delay específico de Integratel
                    return True
            else:
                if self._wait_for_success():
                    logger.info("Login exitoso")
                    return True

            # Extraer mensaje de error si existe
            lock_message = self._extract_portal_message()
            if lock_message:
                logger.error("Login rechazado por el portal: %s", lock_message)
                raise RuntimeError(lock_message)

            message = "Login falló: no se alcanzó la URL de destino."
            logger.error("%s", message)
            raise RuntimeError(message)

        except Exception as exc:
            message = f"Error durante el login: {exc}"
            logger.error("%s", message, exc_info=True)
            raise RuntimeError(message) from exc

    def is_logged_in(self) -> bool:
        """
        Verificación rápida de sesión activa.

        Returns:
            True si la sesión está activa
        """
        try:
            return self.success_url_contains in self.driver.current_url
        except Exception:
            return False

    def verify_login_success(self, max_wait_seconds: int = 60) -> bool:
        """
        Comprueba que la URL actual corresponde al homepage esperado (método legacy).

        Este método incluye lógica específica para Integratel.

        Args:
            max_wait_seconds: Tiempo máximo de espera

        Returns:
            True si el login fue exitoso
        """
        try:
            verification_wait = WebDriverWait(self.driver, max_wait_seconds)
            verification_wait.until(EC.url_contains(self.success_url_contains))

            # Dar un momento adicional para asegurar que la página está completamente cargada
            time.sleep(2)

            current = self.driver.current_url
            logger.debug("URL actual después del login: %s", current)

            if "dspcas/login" in current:
                logger.warning("Aún en página de login, el login puede haber fallado")
                return False

            if self.success_url_contains in current:
                logger.debug("URL de éxito confirmada: %s", current)
                return True

            logger.warning("URL no coincide con el patrón esperado")
            return False
        except TimeoutException:
            current_url = self.driver.current_url
            logger.error("Timeout esperando redirección. URL actual: %s", current_url)
            return False
        except Exception as exc:
            logger.error("Error verificando login: %s", exc, exc_info=True)
            return False

    # ============================
    # Internal helpers
    # ============================

    def _fill_credentials(self, username: str, password: str) -> None:
        """Llena los campos de usuario y contraseña."""
        self.wait.until(EC.visibility_of_element_located(self.selectors.username)).send_keys(username)
        self.wait.until(EC.visibility_of_element_located(self.selectors.password)).send_keys(password)

    def _submit_login(self) -> None:
        """Envía el formulario de login (modo genérico)."""
        button = self.wait.until(EC.element_to_be_clickable(self.selectors.submit))
        button.click()

        # Esperar navegación real
        try:
            self.wait.until(EC.staleness_of(button))
        except TimeoutException:
            logger.debug("Botón no se invalidó, posible SPA")

    def _submit_login_legacy(self) -> None:
        """Envía el formulario de login (modo legacy con lógica específica de Integratel)."""
        login_button = self.wait.until(
            EC.element_to_be_clickable(self.selectors.submit)
        )
        login_button.click()
        try:
            self.wait.until(EC.staleness_of(login_button))
        except TimeoutException:
            logger.debug("Botón de login sigue activo, intentando segundo clic")
            second_button = self.wait.until(
                EC.element_to_be_clickable(self.selectors.submit)
            )
            second_button.click()

    def _wait_for_success(self) -> bool:
        """Espera a que se alcance la URL de éxito (modo genérico)."""
        try:
            self.wait.until(EC.url_contains(self.success_url_contains))
            return True
        except TimeoutException:
            logger.warning("No se alcanzó URL de éxito")
            return False

    def _extract_portal_message(self) -> Optional[str]:
        """
        Extrae mensajes visibles del portal (errores, bloqueos, etc.).

        En modo legacy, busca "user" y "locked" juntos.
        En modo genérico, busca keywords individuales.
        """
        try:
            text = self.driver.execute_script(
                "return document.body?.innerText || document.body.innerText || ''"
            ).strip()
        except Exception:
            return None

        if not text:
            return None

        if self._is_legacy:
            # Lógica específica de Integratel: buscar "user" y "locked" juntos
            if all(keyword in text.lower() for keyword in ("user", "locked")):
                return text.splitlines()[0]
        else:
            # Lógica genérica: buscar keywords individuales
            for keyword in ("locked", "invalid", "error"):
                if keyword in text.lower():
                    return text.splitlines()[0]

        return None
