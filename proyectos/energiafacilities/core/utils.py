from __future__ import annotations
import logging
from envyaml import EnvYAML
from pathlib import Path
from dotenv import load_dotenv
import os
import json
from typing import Dict, Any, Optional,List
import sys
import shutil
from datetime import datetime, date, timedelta
from . import helpers as core_helpers

logger = logging.getLogger(__name__)

 # Funciones globales
def _get_logging_level_from_airflow() -> str | None:
    """
    Obtiene el nivel de logging desde Airflow Variable LOGGING_LEVEL.

    Returns:
        Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL) o None si no está disponible
    """
    try:
        from airflow.sdk import Variable
        level = Variable.get("LOGGING_LEVEL", default_var=None)
        if level:
            return level.upper()
    except ImportError:
        pass  # Airflow no disponible
    except Exception:
        pass  # Variable no existe o error
    return None


def setup_logging(level: str = None) -> None:
    """
    Configura el logging para el proyecto.

    El nivel se determina en el siguiente orden de prioridad:
    1. Airflow Variable LOGGING_LEVEL (si está en Airflow)
    2. Variable de entorno LOGGING_LEVEL
    3. Parámetro `level` pasado a la función
    4. Por defecto: INFO

    Args:
        level: Nivel de logging opcional (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Determinar nivel con prioridad: Airflow > env var > parámetro > default
    resolved_level = (
        _get_logging_level_from_airflow()
        or os.getenv("LOGGING_LEVEL")
        or level
        or "INFO"
    ).upper()

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    handler.setFormatter(formatter)
    logging.basicConfig(
        level=getattr(logging, resolved_level, logging.INFO),
        handlers=[handler]
    )
    # Silenciar logs molestos de librerías externas
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def _is_airflow_available() -> bool:
    """
    Detecta si Airflow está disponible en el entorno actual.
    
    Returns:
        True si Airflow está disponible, False en caso contrario
    """
    try:
        from airflow.sdk import Variable  # noqa: F401
        from airflow.sdk.bases.hook import BaseHook  # noqa: F401
        return True
    except ImportError:
        return False


def _get_env_from_airflow() -> Optional[str]:
    """
    Obtiene el entorno (ENV_MODE) desde Airflow Variable.
    
    Returns:
        Valor de ENV_MODE desde Airflow Variable, o None si no está disponible
    """
    if not _is_airflow_available():
        return None
    
    try:
        from airflow.sdk import Variable
        try:
            env = Variable.get("ENV_MODE")
            if env:
                logger.debug(f"ENV_MODE obtenido desde Airflow Variable: {env}")
                return env
        except KeyError:
            # Variable no existe en Airflow
            logger.debug("Variable ENV_MODE no existe en Airflow")
    except Exception as exc:
        logger.debug(f"No se pudo obtener ENV_MODE desde Airflow: {exc}")
    
    return None


def _load_airflow_connection(conn_id: str, env: str = None) -> Dict[str, Any]:
    """
    Carga valores desde una Airflow Connection.
    Intenta primero con Connection específica por entorno, luego genérica.
    
    Args:
        conn_id: ID base de la conexión de Airflow (ej: "teleows_portal")
        env: Entorno actual (dev, staging, prod) para buscar Connection específica
        
    Returns:
        Diccionario con valores de la conexión (username, password, extras)
    """
    if not _is_airflow_available():
        return {}
    
    values: Dict[str, Any] = {}
    
    # Lista de Connection IDs a intentar (específica por entorno primero, luego genérica)
    conn_ids_to_try = []
    if env:
        conn_ids_to_try.append(f"{conn_id}_{env}")  # Ej: teleows_portal_prod
    conn_ids_to_try.append(conn_id)  # Ej: teleows_portal (genérica)
    
    try:
        from airflow.sdk.bases.hook import BaseHook
        
        fallback_conn_ids = {"sftp_base_sitios", "sftp_base_sitios_bitacora", "sftp_clientes_libres", "sftp_toa"}
        for conn_id_to_try in conn_ids_to_try:
            try:
                conn = BaseHook.get_connection(conn_id_to_try)
                
                # Mapear campos básicos comunes
                if conn.login:
                    values["username"] = conn.login  # Para SFTP/HTTP
                if conn.password:
                    values["password"] = conn.password
                
                # Mapear campos comunes a todos los tipos de conexión
                if conn.host:
                    values["host"] = conn.host
                if conn.port:
                    values["port"] = conn.port
                
                # Mapear campos específicos por tipo de conexión
                if conn.conn_type == "postgres":
                    if conn.schema:
                        # En conexiones PostgreSQL, schema puede ser el nombre de la base de datos
                        values["schema"] = conn.schema
                        if "database" not in values:
                            values["database"] = conn.schema
                    # PostgreSQL necesita "user" además de "username"
                    if conn.login:
                        values["user"] = conn.login
                
                # Cargar extras del connection (puede contener más configuraciones)
                extras = getattr(conn, "extra_dejson", {}) or {}
                if isinstance(extras, dict):
                    values.update(extras)
                    # Mapear campos con prefijo "default_" a nombres estándar para compatibilidad
                    if "default_remote_dir" in extras and "remote_dir" not in values:
                        values["remote_dir"] = extras["default_remote_dir"]
                    if "default_local_dir" in extras and "local_dir" not in values:
                        values["local_dir"] = extras["default_local_dir"]
                    # Mapear processed_destination a local_destination_dir para clientes_libres
                    if "processed_destination" in extras and "local_destination_dir" not in values:
                        values["local_destination_dir"] = extras["processed_destination"]
                    
                    # Mapeos específicos para webindra (conexiones HTTP)
                    if conn.conn_type == "http" and ("http_webindra" in conn_id_to_try or "webindra" in conn_id_to_try):
                        # Mapear host a BASE_URL
                        if conn.host and "BASE_URL" not in values:
                            values["BASE_URL"] = conn.host
                        # Mapear campos de extras a mayúsculas para webindra
                        if "headers" in extras and "HEADERS" not in values:
                            values["HEADERS"] = extras["headers"]
                        if "export_template" in extras and "EXPORT_TMPL" not in values:
                            values["EXPORT_TMPL"] = extras["export_template"]
                        if "login_path" in extras and "LOGIN_PATH" not in values:
                            values["LOGIN_PATH"] = extras["login_path"]
                        if "period_months" in extras and "PERIOD_MONTHS" not in values:
                            values["PERIOD_MONTHS"] = extras["period_months"]
                        if "max_retries" in extras and "MAX_RETRIES" not in values:
                            values["MAX_RETRIES"] = extras["max_retries"]
                        if "timeout" in extras and "TIMEOUT" not in values:
                            values["TIMEOUT"] = extras["timeout"]
                        if "proxy" in extras and "PROXY" not in values:
                            values["PROXY"] = extras["proxy"]
                        # Mapear local_dir si está disponible
                        if "local_dir" in extras and "local_dir" not in values:
                            values["local_dir"] = extras["local_dir"]
                        # Mapear login y password a USER y PASS
                        if conn.login and "USER" not in values:
                            values["USER"] = conn.login
                        if conn.password and "PASS" not in values:
                            values["PASS"] = conn.password
                    
                logger.debug(f"Valores cargados desde Connection '{conn_id_to_try}': {list(values.keys())}")

                # Si faltan credenciales en conexiones DAAS específicas, usar sftp_daas_{env}
                if conn_id in fallback_conn_ids and (not values.get("username") or not values.get("password")):
                    daas_conn_ids = []
                    if env:
                        daas_conn_ids.append(f"sftp_daas_{env}")
                    daas_conn_ids.append("sftp_daas")
                    for daas_conn_id in daas_conn_ids:
                        try:
                            daas_conn = BaseHook.get_connection(daas_conn_id)
                            if not values.get("username") and daas_conn.login:
                                values["username"] = daas_conn.login
                            if not values.get("password") and daas_conn.password:
                                values["password"] = daas_conn.password
                            if not values.get("host") and daas_conn.host:
                                values["host"] = daas_conn.host
                            if not values.get("port") and daas_conn.port:
                                values["port"] = daas_conn.port
                            logger.debug(f"Credenciales fallback desde '{daas_conn_id}' para '{conn_id_to_try}'")
                            break
                        except Exception:
                            continue
                break  # Si encontramos la Connection, salir del loop
                
            except Exception:
                # Si no existe esta Connection, intentar la siguiente
                continue
        
    except Exception as exc:
        logger.debug(f"No se pudo obtener ninguna conexión para '{conn_id}': {exc}")
    
    return values


def _load_airflow_variables(prefix: str, section_name: str = None, env: str = None) -> Dict[str, Any]:
    """
    Carga Variables de Airflow con un prefijo específico.
    Intenta primero con Variables específicas por entorno, luego genéricas.
    
    Args:
        prefix: Prefijo de las variables (ej: "TELEOWS_", "POSTGRES_")
        section_name: Nombre de la sección para mapeo adicional (opcional)
        env: Entorno actual (dev, staging, prod) para buscar Variables específicas
        
    Returns:
        Diccionario con valores de las Variables (sin el prefijo en las keys)
    """
    if not _is_airflow_available():
        return {}
    
    values: Dict[str, Any] = {}
    
    try:
        from airflow.sdk import Variable
        
        # Mapeo de campos comunes por sección
        common_fields = {
            "teleows": ["username", "password", "proxy", "download_path", "max_iframe_attempts", 
                       "max_status_attempts", "options_to_select", "export_overwrite_files", "headless"],
            "postgress": ["user", "password", "host", "port", "database"],
            "sftp_energia_c": ["host", "port", "username", "password"],
            "sftp_daas_c": ["host", "port", "username", "password"],
            "sftp_energia": ["remote_dir", "local_dir", "specific_filename", "specific_filename2", 
                           "schema", "table_DA", "table_PD", "if_exists", "sp_carga_DA", "sp_carga_PD"],
            "sftp_base_sitios": ["remote_dir", "local_dir", "schema", "table", "if_exists", 
                               "specific_filename", "sp_carga"],
            "sftp_base_sitios_bitacora": ["remote_dir", "local_dir", "schema", "table", "if_exists", 
                                        "specific_filename", "sp_carga"],
            "webindra_energia": ["BASE_URL", "LOGIN_PATH", "EXPORT_TMPL", "USER", "PASS", 
                               "PERIOD_MONTHS", "TIMEOUT", "MAX_RETRIES", "PROXY", "HEADERS", 
                               "local_dir", "specific_filename", "schema", "table", "if_exists", "sp_carga"],
            "clientes_libres": ["remote_dir", "local_dir", "specific_filename", "local_destination_dir", 
                              "schema", "table", "if_exists", "sp_carga"],
            "gde": ["date_mode", "date_from", "date_to", "last_n_days", "specific_filename", 
                   "local_dir", "schema", "table", "if_exists", "chunksize", "sp_carga"],
            "dynamic_checklist": ["date_mode", "date_from", "date_to", "last_n_days", 
                                 "specific_filename", "local_dir", "schema", "table", 
                                 "load_mode", "chunksize", "sp_carga"],
        }
        
        # Obtener campos a buscar
        fields_to_check = common_fields.get(section_name, []) if section_name else []
        
        # También intentar con prefijo genérico para campos comunes
        generic_fields = ["proxy", "host", "port", "username", "password", "user", 
                         "database", "download_path", "local_dir"]
        
        all_fields = list(set(fields_to_check + generic_fields))
        
        for field in all_fields:
            # Intentar primero con sufijo de entorno, luego genérica
            var_names_to_try = []
            if env:
                # Variable específica por entorno: TELEOWS_USERNAME_PROD (formato estándar: PREFIX_FIELD_ENV)
                var_names_to_try.append(f"{prefix}{field.upper()}_{env.upper()}")
            # Variable genérica: TELEOWS_USERNAME
            var_names_to_try.append(f"{prefix}{field.upper()}")
            
            for var_name in var_names_to_try:
                try:
                    value = Variable.get(var_name)
                    # Convertir strings a tipos apropiados si es necesario
                    if isinstance(value, str):
                        value_lower = value.lower()
                        if value_lower in ("true", "false"):
                            values[field] = value_lower == "true"
                        elif value.isdigit():
                            values[field] = int(value)
                        elif field.upper() == "HEADERS":
                            # Si es HEADERS, parsear como JSON string desde Variables de Airflow
                            try:
                                values[field] = json.loads(value)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Error al parsear HEADERS como JSON desde '{var_name}': {e}. Usando valor como string.")
                                values[field] = value
                        else:
                            values[field] = value
                    else:
                        # Si ya es un tipo no-string (int, bool, dict, etc.), usarlo directamente
                        # Si es HEADERS y ya es un dict, usarlo tal cual (viene de YAML)
                        values[field] = value
                    break  # Si encontramos la variable, salir del loop y pasar al siguiente campo
                except KeyError:
                    continue  # Intentar siguiente nombre de variable
                except Exception as exc:
                    logger.debug(f"No se pudo leer la Variable '{var_name}': {exc}")
                    break  # Si hay otro error, pasar al siguiente campo
                
    except Exception as exc:
        logger.debug(f"Error al cargar Variables de Airflow con prefijo '{prefix}': {exc}")
    
    return values


class LazyAirflowConfig(dict):
    """
    Diccionario que hace lazy loading de secciones desde Airflow Connections/Variables.
    Permite que el autodescubrimiento funcione incluso cuando no hay YAML ni mapping explícito.

    Cuando se solicita una sección con .get() o [], si no existe en el diccionario,
    automáticamente intenta cargarla desde Airflow usando autodescubrimiento.
    """

    def __init__(self, *args, env: str = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._env = env
        self._loading = set()  # Para evitar recursión infinita

    def _load_section_from_airflow(self, section_name: str) -> dict:
        """
        Intenta cargar una sección desde Airflow usando autodescubrimiento.

        Args:
            section_name: Nombre de la sección a cargar (ej: "sftp_toa")

        Returns:
            Diccionario con la configuración cargada, o {} si no se pudo cargar
        """
        if not _is_airflow_available():
            return {}

        # Evitar recursión infinita
        if section_name in self._loading:
            return {}

        self._loading.add(section_name)
        try:
            # Auto-descubrimiento: usar el nombre de la sección como conn_id
            conn_id = section_name
            var_prefix = f"{section_name.upper()}_"

            logger.debug(f"Lazy loading para sección '{section_name}' usando autodescubrimiento (conn_id='{conn_id}', var_prefix='{var_prefix}')")

            section_config = {}

            # 1. Cargar desde Connection
            conn_values = _load_airflow_connection(conn_id, self._env)
            if conn_values and isinstance(conn_values, dict):
                section_config.update(conn_values)
                logger.debug(f"Valores cargados desde Connection '{conn_id}_{self._env}' para sección '{section_name}': {list(conn_values.keys())}")

            # 2. Cargar desde Variables
            var_values = _load_airflow_variables(var_prefix, None, self._env)
            if var_values and isinstance(var_values, dict):
                section_config.update(var_values)
                logger.debug(f"Valores cargados desde Variables '{var_prefix}*' para sección '{section_name}': {list(var_values.keys())}")

            if section_config:
                logger.debug(f"Sección '{section_name}' cargada exitosamente desde Airflow mediante autodescubrimiento")
            else:
                logger.debug(f"No se encontró configuración para sección '{section_name}' en Airflow")

            return section_config
        finally:
            self._loading.discard(section_name)

    def __getitem__(self, key):
        """Sobrescribe [] para hacer lazy loading."""
        if key not in self:
            # Intentar cargar desde Airflow
            section_config = self._load_section_from_airflow(key)
            if section_config:
                super().__setitem__(key, section_config)
                return section_config
            # Si no se pudo cargar, dejar que KeyError se lance normalmente
            raise KeyError(key)
        return super().__getitem__(key)

    def get(self, key, default=None):
        """Sobrescribe get() para hacer lazy loading."""
        if key not in self:
            # Intentar cargar desde Airflow
            section_config = self._load_section_from_airflow(key)
            if section_config:
                super().__setitem__(key, section_config)
                return section_config
            return default
        return super().get(key, default)


def _apply_airflow_overrides(config: dict, env: str) -> dict:
    """
    Aplica overrides desde Airflow Connections y Variables al config cargado desde YAML.
    Si el config está vacío o no tiene secciones, las construye desde Airflow.

    Prioridad: Variables > Connection > YAML

    Args:
        config: Configuración cargada desde YAML (puede estar vacío si no existe YAML)
        env: Entorno actual (dev, staging, prod)

    Returns:
        Configuración con overrides aplicados desde Airflow
    """
    if not _is_airflow_available():
        logger.debug("Airflow no disponible, retornando config YAML sin modificaciones")
        return config
    
    # Mapeo de secciones YAML a Connections y prefijos de Variables
    # Los conn_id se resuelven automáticamente con sufijo de entorno (ej: postgres_siom -> postgres_siom_dev)
    section_mapping = {
        "teleows": {
            "conn_id": "generic_autin_shared",  # Se resuelve a generic_autin_shared_{env}
            "var_prefix": "TELEOWS_"
        },
        "postgress": {
            "conn_id": "postgres_siom",  # Se resuelve a postgres_siom_{env}
            "var_prefix": "POSTGRES_"
        },
        "sftp_energia_c": {
            "conn_id": "sftp_energia",  # Se resuelve a sftp_energia_{env}
            "var_prefix": "SFTP_ENERGIA_"
        },
        "sftp_energia": {
            "conn_id": "sftp_energia",  # Lee extras desde la misma Connection
            "var_prefix": "SFTP_ENERGIA_"
        },
        "sftp_daas_c": {
            "conn_id": "sftp_daas",  # Se resuelve a sftp_daas_{env}
            "var_prefix": "SFTP_DAAS_"
        },
        "sftp_base_sitios": {
            "conn_id": "sftp_base_sitios",  # Se resuelve a sftp_base_sitios_{env}
            "var_prefix": "SFTP_BASE_SITIOS_"
        },
        "sftp_base_sitios_bitacora": {
            "conn_id": "sftp_base_sitios_bitacora",  # Se resuelve a sftp_base_sitios_bitacora_{env}
            "var_prefix": "SFTP_BASE_SITIOS_BITACORA_"
        },
        "webindra_energia": {
            "conn_id": "http_webindra",  # Se resuelve a http_webindra_{env}
            "var_prefix": "WEBINDRA_ENERGIA_"
        },
        "clientes_libres": {
            "conn_id": "sftp_clientes_libres",  # Se resuelve a sftp_clientes_libres_{env}
            "var_prefix": "CLIENTES_LIBRES_"
        },
        "gde": {
            "conn_id": "generic_autin_gde",  # Se resuelve a generic_autin_gde_{env}
            "var_prefix": "TELEOWS_GDE_"  # Prefijo específico para GDE
        },
        "dynamic_checklist": {
            "conn_id": "generic_autin_dc",  # Se resuelve a generic_autin_dc_{env}
            "var_prefix": "TELEOWS_DC_"  # Prefijo específico para Dynamic Checklist
        },
    }
    
    # Crear copia del config para no modificar el original
    merged_config = dict(config)
    
    # Solo tomar del YAML las secciones que realmente son diccionarios.
    yaml_dict_sections = {k for k, v in merged_config.items() if isinstance(v, dict)}
    skipped_sections = {k for k, v in merged_config.items() if not isinstance(v, dict)}
    if skipped_sections:
        logger.debug(
            "Ignorando %d secciones tipo escalar al aplicar overrides: %s",
            len(skipped_sections),
            ", ".join(sorted(skipped_sections)),
        )

    # Unir las secciones válidas del YAML con las registradas explícitamente.
    all_sections = yaml_dict_sections | set(section_mapping.keys())
    
    # Aplicar overrides por sección (o construir desde cero si no existe)
    for section_name in all_sections:
        # Si la sección no existe en el config, inicializarla vacía
        if section_name not in merged_config:
            merged_config[section_name] = {}
            logger.debug(f"Sección '{section_name}' no existe en YAML, construyendo desde Airflow")
        
        # Asegurar que section_config sea un diccionario
        section_value = merged_config.get(section_name)
        if isinstance(section_value, dict):
            section_config = dict(section_value)
        else:
            logger.warning(f"Sección '{section_name}' no es un diccionario (tipo: {type(section_value)}), inicializando como diccionario vacío")
            section_config = {}
        
        # Obtener mapeo para esta sección (si existe), o usar auto-descubrimiento
        mapping = section_mapping.get(section_name)
        
        if mapping:
            # Sección registrada: usar mapeo explícito
            conn_id = mapping.get("conn_id")
            var_prefix = mapping.get("var_prefix")
        else:
            # Sección NO registrada: auto-descubrimiento basado en convenciones
            # Por defecto, intentar buscar Connection con el mismo nombre que la sección
            conn_id = section_name
            # Para Variables, usar prefijo basado en el nombre de la sección (ej: "nuevo_modulo" -> "NUEVO_MODULO_")
            var_prefix = f"{section_name.upper()}_"
            logger.debug(f"Sección '{section_name}' no está en mapeo, usando auto-descubrimiento (conn_id='{conn_id}', var_prefix='{var_prefix}')")
        
        # 1. Cargar desde Connection (si existe y está configurada)
        # Intenta primero Connection específica por entorno, luego genérica
        if conn_id:
            conn_values = _load_airflow_connection(conn_id, env)
            if conn_values and isinstance(conn_values, dict):
                # Merge: Connection sobrescribe YAML
                section_config.update(conn_values)
                logger.debug(f"Overrides desde Connection '{conn_id}' (env={env}) aplicados a sección '{section_name}'")
            elif conn_values:
                logger.warning(f"Connection '{conn_id}' devolvió un valor no-diccionario (tipo: {type(conn_values)}), ignorando")
        
        # 2. Cargar desde Variables (sobrescriben Connection)
        # Si hay un mapeo específico, usar sus campos. Si no, usar solo campos genéricos
        var_values = _load_airflow_variables(var_prefix, section_name if mapping else None, env)
        
        # Para secciones especiales, también cargar desde prefijo genérico
        if section_name in ("gde", "dynamic_checklist"):
            generic_values = _load_airflow_variables("TELEOWS_", section_name, env)
            # Asegurar que ambos sean diccionarios antes de hacer update
            if isinstance(generic_values, dict) and isinstance(var_values, dict):
                # Merge: prefijo específico sobrescribe genérico
                generic_values.update(var_values)
                var_values = generic_values
            elif isinstance(generic_values, dict):
                var_values = generic_values
            elif not isinstance(var_values, dict):
                var_values = {}
        
        if var_values and isinstance(var_values, dict):
            # Merge: Variables sobrescriben Connection y YAML
            section_config.update(var_values)
        elif var_values:
            logger.warning(f"Variables para '{section_name}' devolvieron un valor no-diccionario (tipo: {type(var_values)}), ignorando")
        
        # Actualizar la sección en el config final (solo si tiene valores)
        if section_config:
            merged_config[section_name] = section_config
    
    return merged_config


def load_config(env: str | None = None) -> dict:
    """
    Carga un archivo YAML con soporte automático para variables de entorno
    y priorización de Airflow Connections/Variables.
    
    Prioridad de configuración:
    1. Airflow Variables (si está en Airflow)
    2. Airflow Connections (si está en Airflow)
    3. Variables de entorno (${VAR_NAME} en YAML)
    4. Valores del YAML
    
    El entorno se determina automáticamente:
    1. Parámetro env (si se pasa explícitamente)
    2. Airflow Variable ENV_MODE (si está en Airflow)
    3. Variable de entorno ENV_MODE
    4. Por defecto "dev"
    
    Ejemplo de uso en el YAML:
        postgres:
          user: ${POSTGRES_USER}
          password: ${POSTGRES_PASS}
    
    Si las variables existen en el entorno, se reemplazan automáticamente.
    Si estamos en Airflow, los valores de Connections/Variables tienen prioridad.

    """
    try:
        # Determinar directorio base de energiafacilities
        base_dir = Path(__file__).resolve().parent.parent

        # Cargar variables del .env de energiafacilities si existe
        dotenv_path = base_dir / ".env"
        if dotenv_path.exists():
            logger.debug(f"Cargando variables de entorno desde: {dotenv_path}")
            load_dotenv(dotenv_path=dotenv_path, override=False)
        else:
            logger.debug("No se encontró archivo .env en energiafacilities, usando variables del sistema")
            load_dotenv()  # Buscar en directorio actual o padres

        # Determinar entorno con prioridad: parámetro > Airflow Variable > env var > default
        if env:
            resolved_env = env.lower()
            logger.debug(f"Entorno especificado explícitamente: {resolved_env}")
        else:
            # Intentar obtener desde Airflow Variable primero
            airflow_env = _get_env_from_airflow()
            if airflow_env:
                resolved_env = airflow_env.lower()
                logger.debug(f"Entorno obtenido desde Airflow Variable ENV_MODE: {resolved_env}")
            else:
                resolved_env = os.getenv("ENV_MODE", "dev").lower()
                logger.debug(f"Entorno obtenido desde variable de entorno ENV_MODE: {resolved_env}")
        
        config_path = base_dir / "config" / f"config_{resolved_env}.yaml"

        # Cargar YAML si existe, sino crear config vacío (se construirá desde Airflow)
        if not os.path.exists(config_path):
            if _is_airflow_available():
                logger.warning(f"No existe el archivo de configuración: {config_path}")
                logger.debug("Construyendo configuración completamente desde Airflow Variables/Connections")
                yaml_config = {}  # Config vacío, se construirá desde Airflow
            else:
                logger.error(f"No existe el archivo de configuración: {config_path}")
                raise FileNotFoundError(f"No existe el archivo de configuración: {config_path} y Airflow no está disponible")
        else:
            # Cargar YAML con envyaml (hace el reemplazo automático de variables de entorno)
            logger.debug(f"Cargando configuración desde: {config_path}")
            cfg = EnvYAML(config_path, strict=False)
            yaml_config = dict(cfg)
        
        # Aplicar overrides desde Airflow (si está disponible)
        # Esto sobrescribe valores del YAML con valores de Connections/Variables
        # Si el YAML está vacío, construye toda la configuración desde Airflow
        final_config = _apply_airflow_overrides(yaml_config, resolved_env)

        logger.debug(f"Configuración cargada para entorno '{resolved_env}'. "
                    f"Airflow disponible: {_is_airflow_available()}")

        # Devolver LazyAirflowConfig para permitir autodescubrimiento dinámico
        # de secciones que no están en YAML ni en section_mapping
        return LazyAirflowConfig(final_config, env=resolved_env)

    except FileNotFoundError as e:
        logger.error(f"No se encontró el archivo: {e}")
        raise
    except Exception as e:
        logger.error(f"Error al cargar configuración: {e}")
        raise


def asegurar_directorio_sftp(sftp, ruta_completa):
    return core_helpers.asegurar_directorio_sftp(sftp, ruta_completa)


def traerjson(archivo='',valor=None):
    return core_helpers.traerjson(archivo=archivo, valor=valor)



def borrar_ruta(ruta: str):
    return core_helpers.borrar_ruta(ruta)
        


 # Funciones especificos de SFTP energia:
def generar_archivo_especifico(
    lista_archivos: List[Dict[str, str | datetime]],
    basearchivo: Optional[str] = None,
    periodo: Optional[str] = None,
    tipo: Optional[str] = None
) -> Optional[Dict[str, str | datetime]]:
    return core_helpers.generar_archivo_especifico(
        lista_archivos=lista_archivos,
        basearchivo=basearchivo,
        periodo=periodo,
        tipo=tipo,
    )


def archivoespecifico_periodo(
    lista_archivos: List[str],
    basearchivo: Optional[str] = None,
    periodo: Optional[str] = None,
    tipo: Optional[str] = None
    ):
    return core_helpers.archivoespecifico_periodo(
        lista_archivos=lista_archivos,
        basearchivo=basearchivo,
        periodo=periodo,
        tipo=tipo,
    )

def archivoespecifico_periodo_CL(
    lista_archivos: List[str],
    basearchivo: Optional[str] = None,
    periodo: Optional[str] = None,
    tipo: Optional[str] = None
    ):
    return core_helpers.archivoespecifico_periodo_CL(
        lista_archivos=lista_archivos,
        basearchivo=basearchivo,
        periodo=periodo,
        tipo=tipo,
    )



#Crea carpeta si no existe
def crearcarpeta(local_dir: str):
    return core_helpers.crearcarpeta(local_dir)


def default_download_path() -> str:
    return core_helpers.default_download_path()
