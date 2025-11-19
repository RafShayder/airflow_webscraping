import time
import hashlib
import logging
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from pathlib import Path
import urllib3

from energiafacilities.core.utils import load_config 

# Desactivar warnings de SSL cuando se usa verify=False (necesario para proxies corporativos)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


# ===========================
# CONFIG LOGGING
# ===========================


# ===========================
# FUNCIONES AUXILIARES
# ===========================

def prepare_period(n_months: int) -> tuple[int, int]:
    """Devuelve epoch (inicio, fin) UTC de los últimos n meses completos."""
    def month_start(dt): return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    def add_months(dt, n):
        y, m = divmod(dt.month - 1 + n, 12)
        return dt.replace(year=dt.year + y, month=m + 1)

    now = datetime.now(timezone.utc)
    this_month = month_start(now)
    start = add_months(this_month, -n_months)
    return int(start.timestamp()), int(this_month.timestamp())


def detect_csrf(html: str) -> dict:
    """Busca campos CSRF/token en el HTML."""
    soup = BeautifulSoup(html, "html.parser")
    return {
        inp.get("name"): inp.get("value") or ""
        for inp in soup.find_all("input", {"type": "hidden"})
        if "csrf" in (inp.get("name") or "").lower() or "token" in (inp.get("name") or "").lower()
    }


def sha12(data: bytes) -> str:
    """Hash corto para el nombre del archivo."""
    return hashlib.sha256(data).hexdigest()[:12]


# ===========================
# SCRAPER PRINCIPAL
# ===========================

def run_scraper(cfg: dict) -> Path:
    """Flujo principal: conectividad → login → descarga → guardado."""
    session = requests.Session()
    session.headers.update(cfg["HEADERS"])
    
    # 🔹 Configurar proxy si está disponible
    proxy = cfg.get("PROXY") or os.getenv("PROXY")
    proxies = None
    if proxy:
        proxy_url = proxy if "://" in proxy else f"http://{proxy}"
        proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }
        logger.debug(f"Configurando proxy: {proxy_url}")

    # 🔹 Verificar conectividad
    try:
        logger.debug(f"Verificando conectividad con {cfg['BASE_URL']}")
        resp = session.head(cfg["BASE_URL"], timeout=10, proxies=proxies, verify=False)
        if resp.status_code >= 400:
            raise ConnectionError(f"Conectividad fallida (HTTP {resp.status_code})")
        logger.debug("Conectividad verificada correctamente")
    except Exception as e:
        logger.error(f"Error de conexión a {cfg['BASE_URL']}: {e}")
        raise

    # 🔹 Login
    try:
        login_url = f"{cfg['BASE_URL'].rstrip('/')}{cfg['LOGIN_PATH']}"
        logger.debug(f"Iniciando sesión en {login_url}")
        r = session.get(login_url, timeout=cfg["TIMEOUT"], proxies=proxies, verify=False)
        r.raise_for_status()

        csrf = detect_csrf(r.text)
        payload = {"username": cfg["USER"], "password": cfg["PASS"], **csrf}

        r2 = session.post(login_url, data=payload, allow_redirects=True, timeout=cfg["TIMEOUT"], proxies=proxies, verify=False)
        r2.raise_for_status()

        if not any(c.name == "ci_session" for c in session.cookies):
            raise RuntimeError("Login fallido: no se estableció cookie de sesión")

        logger.info("Login exitoso")

    except Exception as e:
        logger.error(f"Error en login: {e}")
        raise

    # 🔹 Descarga del archivo
    start, end = prepare_period(cfg["PERIOD_MONTHS"])
    url = f"{cfg['BASE_URL'].rstrip('/')}{cfg['EXPORT_TMPL'].format(start=start, end=end)}"

    data = None
    for attempt in range(1, cfg.get("MAX_RETRIES", 3) + 1):
        try:
            logger.debug(f"Descargando archivo (intento {attempt}) desde {url}")
            r = session.get(url, timeout=cfg["TIMEOUT"], stream=True, proxies=proxies, verify=False)
            r.raise_for_status()

            data = b"".join(r.iter_content(chunk_size=1024 * 512))
            if not data:
                raise ValueError("Archivo descargado vacío")

            logger.info(f"Descarga exitosa ({len(data)/1024:.1f} KB)")
            break

        except Exception as e:
            if attempt == cfg["MAX_RETRIES"]:
                logger.error(f"Descarga fallida tras {attempt} intentos: {e}")
                raise
            backoff = min(30, 2 ** attempt)
            logger.warning(f"Error descarga ({attempt}): {e}. Reintentando en {backoff}s...")
            time.sleep(backoff)

    # 🔹 Guardar archivo
    try:
        folder = Path(cfg["local_dir"])
        folder.mkdir(parents=True, exist_ok=True)

        # Si se definió nombre específico en config
        if "specific_filename" in cfg and cfg["specific_filename"]:
            name = cfg["specific_filename"]

            if not name.lower().endswith(".xlsx"):
                name += ".xlsx"
        else:
            name = f"recibos_{datetime.now().strftime('%Y%m%d')}_{sha12(data)}.xlsx"

        path = folder / name
        path.write_bytes(data)

        logger.info(f"Archivo guardado correctamente: {path}")
        return str(path)

    except Exception as e:
        logger.error(f"Error al guardar archivo: {e}")
        raise


# ===========================
# USO
# ===========================

def stractor_indra()-> Path | None:
    """Si todo okey, retorna el path del archivo guardado, sino None."""
    
    config = load_config()
    configwebindra = config.get("webindra_energia", {})
    try:
        path = run_scraper(configwebindra)
        logger.info(f"Proceso finalizado correctamente: {path}")
        return path
    except Exception as e:
        logger.error(f"Proceso fallido: {e}")
        raise

