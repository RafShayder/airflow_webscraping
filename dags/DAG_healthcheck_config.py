"""
DAG de healthcheck para validar:
- Variables de Airflow requeridas
- Connections de Airflow requeridas
- Conectividad básica a las fuentes principales (PostgreSQL, SFTP, HTTP)

Ejecución: manual (sin schedule).
"""

import logging
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from airflow.sdk import Variable  # type: ignore

from airflow.sdk.bases.hook import BaseHook
from airflow.exceptions import AirflowNotFoundException

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
import paramiko
import requests


logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    "owner": "SigmaAnalytics",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


# ---------------------------------------------------------------------------
# Funciones para construir listas dinámicas según entorno
# ---------------------------------------------------------------------------

def get_env_mode() -> str:
    """Obtiene el entorno actual desde variable de entorno o Airflow Variable."""
    try:
        env = os.getenv("ENV_MODE") or Variable.get("ENV_MODE", default="dev")
        env = env.lower().strip()
        # Validar que sea un entorno válido
        if env not in ["dev", "staging", "prod"]:
            logger.warning("Entorno '%s' no reconocido, usando 'dev' por defecto", env)
            return "dev"
        return env
    except Exception:
        logger.warning("No se pudo obtener ENV_MODE, usando 'dev' por defecto")
        return "dev"


def get_required_variables(env: str) -> List[str]:
    """Construye la lista de variables requeridas según el entorno."""
    env_upper = env.upper()
    
    # Variables globales (sin sufijo de entorno)
    base_vars = [
        "ENV_MODE",
        "LOGGING_LEVEL",
    ]
    
    # Variables específicas por entorno
    env_vars = [
        # PostgreSQL
        f"POSTGRES_USER_{env_upper}",
        f"POSTGRES_HOST_{env_upper}",
        f"POSTGRES_PORT_{env_upper}",
        f"POSTGRES_DB_{env_upper}",
        # SFTP DAAS
        f"SFTP_HOST_DAAS_{env_upper}",
    ]
    
    # Variable de password con nombre especial para PROD
    if env == "prod":
        env_vars.append("POSTGRES_PASS_PRD")  # Nota: PROD usa "PASS_PRD" no "PASSWORD_PROD"
    else:
        env_vars.append(f"POSTGRES_PASSWORD_{env_upper}")
    
    return base_vars + env_vars


def get_required_connections(env: str) -> List[str]:
    """Construye la lista de connections requeridas según el entorno."""
    return [
        # PostgreSQL (opcional, pero la probamos si existe)
        f"postgres_siom_{env}",
        # SFTP
        f"sftp_daas_{env}",
        f"sftp_base_sitios_{env}",
        f"sftp_base_sitios_bitacora_{env}",
        f"sftp_clientes_libres_{env}",
        f"sftp_toa_{env}",
        f"sftp_energia_{env}",
        # HTTP
        f"http_webindra_{env}",
        # Generic (selenium / teleows)
        f"generic_autin_gde_{env}",
        f"generic_autin_dc_{env}",
    ]


def check_variables() -> Dict[str, Any]:
    """
    Valida la existencia de las Variables requeridas en Airflow.
    Retorna un dict con el detalle por variable.
    """
    # Obtener entorno y construir lista dinámica
    env = get_env_mode()
    required_vars = get_required_variables(env)
    
    logger.info("=" * 80)
    logger.info("VALIDANDO VARIABLES PARA ENTORNO: %s", env.upper())
    logger.info("Variables a validar: %s", required_vars)
    logger.info("=" * 80)
    
    results: Dict[str, Dict[str, Any]] = {}

    for key in required_vars:
        try:
            # No usamos el valor, solo validamos existencia
            Variable.get(key)
            results[key] = {"status": "OK", "message": "Variable encontrada"}
        except AirflowNotFoundException:
            results[key] = {"status": "MISSING", "message": "Variable no encontrada"}
        except Exception as exc:  # fallback genérico
            results[key] = {
                "status": "ERROR",
                "message": f"Error leyendo variable: {exc}",
            }

    ok = [k for k, v in results.items() if v["status"] == "OK"]
    missing = [k for k, v in results.items() if v["status"] == "MISSING"]
    errors = [k for k, v in results.items() if v["status"] == "ERROR"]

    logger.info("Healthcheck Variables -> OK: %s", ok)
    if missing:
        logger.warning("Healthcheck Variables -> MISSING: %s", missing)
    if errors:
        logger.error("Healthcheck Variables -> ERROR: %s", errors)

    return {
        "summary": {
            "ok": len(ok),
            "missing": len(missing),
            "errors": len(errors),
        },
        "detail": results,
    }


def check_connections_exist() -> Dict[str, Any]:
    """
    Valida que existan las connections requeridas en Airflow.
    No prueba conectividad todavía, solo existencia.
    """
    # Obtener entorno y construir lista dinámica
    env = get_env_mode()
    required_conns = get_required_connections(env)
    
    logger.info("=" * 80)
    logger.info("VALIDANDO CONNECTIONS PARA ENTORNO: %s", env.upper())
    logger.info("Connections a validar: %s", required_conns)
    logger.info("=" * 80)
    
    results: Dict[str, Dict[str, Any]] = {}

    for conn_id in required_conns:
        try:
            BaseHook.get_connection(conn_id)
            results[conn_id] = {"status": "OK", "message": "Connection encontrada"}
        except AirflowNotFoundException:
            results[conn_id] = {"status": "MISSING", "message": "Connection no encontrada"}
        except Exception as exc:
            results[conn_id] = {
                "status": "ERROR",
                "message": f"Error obteniendo connection: {exc}",
            }

    ok = [k for k, v in results.items() if v["status"] == "OK"]
    missing = [k for k, v in results.items() if v["status"] == "MISSING"]
    errors = [k for k, v in results.items() if v["status"] == "ERROR"]

    logger.info("Healthcheck Connections -> OK: %s", ok)
    if missing:
        logger.warning("Healthcheck Connections -> MISSING: %s", missing)
    if errors:
        logger.error("Healthcheck Connections -> ERROR: %s", errors)

    return {
        "summary": {
            "ok": len(ok),
            "missing": len(missing),
            "errors": len(errors),
        },
        "detail": results,
    }


def _test_postgres_connection(conn_id: str) -> Dict[str, Any]:
    """Prueba conectividad a PostgreSQL."""
    try:
        logger.info("🔍 Probando conexión PostgreSQL: %s", conn_id)
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        result = pg_hook.get_first(sql="SELECT 1;")
        logger.info("✅ %s: PostgreSQL conectado correctamente", conn_id)
        return {
            "status": "OK",
            "message": f"SELECT 1 ejecutado correctamente (resultado: {result})",
        }
    except Exception as exc:
        error_msg = str(exc)
        logger.error("❌ %s: Error conectando a PostgreSQL - %s", conn_id, error_msg)
        return {
            "status": "ERROR",
            "message": f"Error conectando: {error_msg}",
        }


def _test_sftp_connection(conn_id: str) -> Dict[str, Any]:
    """Prueba conectividad a SFTP usando paramiko directamente (igual que base_stractor.py)."""
    transport = None
    sftp = None
    try:
        logger.info("🔍 Probando conexión SFTP: %s (usando paramiko directamente)", conn_id)
        
        # Obtener la connection de Airflow
        conn = BaseHook.get_connection(conn_id)
        
        # Extraer parámetros de conexión
        host = conn.host
        port = conn.port or 22
        username = conn.login
        password = conn.password
        
        if not all([host, username, password]):
            raise ValueError(f"Faltan parámetros de conexión: host={host}, username={username}, password={'***' if password else None}")
        
        logger.info("   Parámetros: Host=%s, Port=%s, Username=%s", host, port, username)
        logger.info("   Iniciando conexión con paramiko.Transport...")
        
        # Conectar usando paramiko exactamente como en base_stractor.py (sin configuraciones extra)
        transport = paramiko.Transport((host, port))
        logger.info("   Transport creado, conectando...")
        transport.connect(username=username, password=password)
        logger.info("   Transport conectado, creando SFTPClient...")
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        # Probar listando el directorio raíz
        logger.info("   Probando listdir('.')...")
        files = sftp.listdir(".")
        
        logger.info("✅ %s: SFTP conectado correctamente (archivos en raíz: %d)", conn_id, len(files))
        return {
            "status": "OK",
            "message": f"Conexión SFTP establecida y listado básico OK ({len(files)} items en raíz)",
        }
    except paramiko.AuthenticationException as exc:
        error_msg = f"Error de autenticación: {exc}"
        logger.error("❌ %s: %s", conn_id, error_msg)
        return {
            "status": "ERROR",
            "message": error_msg,
        }
    except paramiko.SSHException as exc:
        error_msg = f"Error SSH: {exc}"
        logger.error("❌ %s: %s", conn_id, error_msg)
        return {
            "status": "ERROR",
            "message": error_msg,
        }
    except Exception as exc:
        error_msg = str(exc)
        error_type = type(exc).__name__
        logger.error("❌ %s: Error conectando a SFTP [%s] - %s", conn_id, error_type, error_msg)
        return {
            "status": "ERROR",
            "message": f"Error [{error_type}]: {error_msg}",
        }
    finally:
        # Cerrar conexiones correctamente (igual que en base_stractor.py)
        if sftp:
            try:
                sftp.close()
                logger.debug("   SFTPClient cerrado")
            except Exception:
                pass
        if transport:
            try:
                transport.close()
                logger.debug("   Transport cerrado")
            except Exception:
                pass


def _test_http_connection(conn_id: str) -> Dict[str, Any]:
    """Prueba conectividad a HTTP usando requests directamente (como en webindra/stractor.py)."""
    try:
        logger.info("🔍 Probando conexión HTTP: %s", conn_id)
        
        # Obtener la connection de Airflow
        conn = BaseHook.get_connection(conn_id)
        
        # Extraer parámetros de conexión
        base_url = conn.host
        if not base_url:
            raise ValueError("Connection no tiene 'host' configurado")
        
        # Asegurar que tenga protocolo
        if not base_url.startswith(("http://", "https://")):
            base_url = f"https://{base_url}"
        
        logger.info("   URL base: %s", base_url)
        
        # Obtener headers desde extra (puede ser dict o string JSON)
        headers = {}
        extras = conn.extra_dejson or {}
        if "headers" in extras:
            headers_raw = extras["headers"]
            if isinstance(headers_raw, dict):
                headers = headers_raw
            elif isinstance(headers_raw, str):
                try:
                    headers = json.loads(headers_raw)
                except json.JSONDecodeError:
                    logger.warning("   No se pudo parsear headers como JSON, usando headers por defecto")
                    headers = {"User-Agent": "Mozilla/5.0"}
        else:
            # Headers por defecto si no están configurados
            headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        
        logger.info("   Headers: %s", list(headers.keys()))
        
        # Configurar proxy si está disponible
        proxies = None
        if "proxy" in extras:
            proxy = extras["proxy"]
            if proxy:
                proxy_url = proxy if "://" in proxy else f"http://{proxy}"
                proxies = {"http": proxy_url, "https": proxy_url}
                logger.info("   Proxy: %s", proxy_url)
        
        # Hacer petición HEAD para probar conectividad (más rápido que GET)
        logger.info("   Haciendo petición HEAD a %s...", base_url)
        response = requests.head(
            base_url,
            headers=headers,
            proxies=proxies,
            timeout=10,
            verify=False  # Deshabilitar verificación SSL para pruebas
        )
        
        status_code = response.status_code
        logger.info("   Status code recibido: %d", status_code)
        
        if 200 <= status_code < 400:
            logger.info("✅ %s: HTTP conectado correctamente (status: %d)", conn_id, status_code)
            return {
                "status": "OK",
                "message": f"HTTP {status_code} recibido correctamente",
            }
        else:
            logger.warning("⚠️ %s: HTTP status inesperado: %s", conn_id, status_code)
            return {
                "status": "WARN",
                "message": f"HTTP status inesperado: {status_code}",
            }
    except requests.exceptions.RequestException as exc:
        error_msg = str(exc)
        logger.error("❌ %s: Error de conexión HTTP - %s", conn_id, error_msg)
        return {
            "status": "ERROR",
            "message": f"Error de conexión: {error_msg}",
        }
    except Exception as exc:
        error_msg = str(exc)
        error_type = type(exc).__name__
        logger.error("❌ %s: Error conectando a HTTP [%s] - %s", conn_id, error_type, error_msg)
        return {
            "status": "ERROR",
            "message": f"Error [{error_type}]: {error_msg}",
        }


def _test_generic_connection(conn_id: str) -> Dict[str, Any]:
    """Valida que una conexión Generic se pueda leer correctamente."""
    try:
        logger.info("🔍 Probando conexión Generic: %s", conn_id)
        conn = BaseHook.get_connection(conn_id)
        # Validamos que al menos login/password/extra se puedan acceder sin error
        login = conn.login
        password = conn.password
        extra = conn.extra_dejson
        logger.info("✅ %s: Connection Generic accesible (login: %s, extra keys: %s)", 
                   conn_id, login, list(extra.keys()) if extra else [])
        return {
            "status": "OK",
            "message": f"Connection Generic accesible (login/password/extra leídos correctamente)",
        }
    except Exception as exc:
        error_msg = str(exc)
        logger.error("❌ %s: Error leyendo Connection Generic - %s", conn_id, error_msg)
        return {
            "status": "ERROR",
            "message": f"Error leyendo connection: {error_msg}",
        }


def check_connectivity() -> Dict[str, Any]:
    """
    Prueba conectividad básica a TODAS las connections requeridas.
    
    Identifica automáticamente el tipo de conexión y prueba cada una de forma independiente.
    Si una falla, las demás continúan probándose.
    """
    # Obtener entorno y construir lista dinámica
    env = get_env_mode()
    required_conns = get_required_connections(env)
    
    results: Dict[str, Dict[str, Any]] = {}
    
    logger.info("=" * 80)
    logger.info("INICIANDO PRUEBAS DE CONECTIVIDAD PARA ENTORNO: %s", env.upper())
    logger.info("Total de conexiones a probar: %d", len(required_conns))
    logger.info("=" * 80)

    for conn_id in required_conns:
        logger.info("-" * 80)
        logger.info("Probando conexión: %s", conn_id)
        
        try:
            # Primero verificamos que la connection exista
            conn = BaseHook.get_connection(conn_id)
            conn_type = conn.conn_type.lower() if conn.conn_type else "unknown"
            
            logger.info("Tipo de conexión detectado: %s", conn_type)
            
            # Probamos según el tipo de conexión
            if conn_type == "postgres":
                results[conn_id] = _test_postgres_connection(conn_id)
            elif conn_type == "sftp":
                results[conn_id] = _test_sftp_connection(conn_id)
            elif conn_type in ["http", "https"]:
                results[conn_id] = _test_http_connection(conn_id)
            elif conn_type == "generic":
                results[conn_id] = _test_generic_connection(conn_id)
            else:
                logger.warning("⚠️ %s: Tipo de conexión '%s' no soportado para prueba de conectividad", conn_id, conn_type)
                results[conn_id] = {
                    "status": "SKIPPED",
                    "message": f"Tipo de conexión '{conn_type}' no tiene prueba de conectividad implementada",
                }
                
        except AirflowNotFoundException:
            logger.warning("⚠️ %s: Connection no encontrada, omitiendo prueba", conn_id)
            results[conn_id] = {
                "status": "SKIPPED",
                "message": "Connection no existe en Airflow",
            }
        except Exception as exc:
            error_msg = str(exc)
            logger.error("❌ %s: Error inesperado obteniendo connection - %s", conn_id, error_msg)
            results[conn_id] = {
                "status": "ERROR",
                "message": f"Error obteniendo connection: {error_msg}",
            }

    # Resumen detallado
    logger.info("=" * 80)
    logger.info("RESUMEN DE PRUEBAS DE CONECTIVIDAD")
    logger.info("=" * 80)
    
    ok_conns = [k for k, v in results.items() if v["status"] == "OK"]
    warn_conns = [k for k, v in results.items() if v["status"] == "WARN"]
    error_conns = [k for k, v in results.items() if v["status"] == "ERROR"]
    skipped_conns = [k for k, v in results.items() if v["status"] == "SKIPPED"]
    
    logger.info("✅ CONEXIONES OK (%d): %s", len(ok_conns), ok_conns)
    if warn_conns:
        logger.warning("⚠️ CONEXIONES CON WARNING (%d): %s", len(warn_conns), warn_conns)
    if error_conns:
        logger.error("❌ CONEXIONES CON ERROR (%d): %s", len(error_conns), error_conns)
        for conn_id in error_conns:
            logger.error("   - %s: %s", conn_id, results[conn_id]["message"])
    if skipped_conns:
        logger.info("⏭️ CONEXIONES OMITIDAS (%d): %s", len(skipped_conns), skipped_conns)
    
    summary = {
        "OK": len(ok_conns),
        "WARN": len(warn_conns),
        "ERROR": len(error_conns),
        "SKIPPED": len(skipped_conns),
    }
    
    logger.info("=" * 80)
    logger.info("TOTAL: %d OK, %d WARN, %d ERROR, %d SKIPPED", 
                summary["OK"], summary["WARN"], summary["ERROR"], summary["SKIPPED"])
    logger.info("=" * 80)

    return {"summary": summary, "detail": results}


with DAG(
    "dag_healthcheck_config",
    default_args=DEFAULT_ARGS,
    description="Healthcheck de variables, connections y conectividad a fuentes.",
    schedule=None,
    catchup=False,
    tags=["healthcheck", "config", "integratel"],
) as dag:
    check_vars_task = PythonOperator(
        task_id="check_variables",
        python_callable=check_variables,
    )

    check_conns_task = PythonOperator(
        task_id="check_connections_exist",
        python_callable=check_connections_exist,
    )

    check_connectivity_task = PythonOperator(
        task_id="check_connectivity",
        python_callable=check_connectivity,
    )

    # Orden: primero variables, luego existencia de connections y finalmente pruebas de conectividad
    check_vars_task >> check_conns_task >> check_connectivity_task

