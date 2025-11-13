"""
Utilidades para integración con Apache Airflow.

Este módulo proporciona funciones helper para cargar configuración desde
Airflow Connections y Variables, facilitando la integración de scrapers
con el entorno de Airflow.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def load_overrides_from_airflow(
    fields: List[str],
    conn_id: str = "teleows_portal",
    variable_prefix: str = "TELEOWS_",
) -> Dict[str, Any]:
    """
    Carga overrides desde Airflow Connection y Variables.

    Args:
        fields: Lista de nombres de campos a buscar en Variables
        conn_id: ID de la conexión de Airflow a utilizar
        variable_prefix: Prefijo para las variables de Airflow

    Returns:
        Diccionario con overrides para *Config.from_yaml_config()

    Ejemplo:
        >>> gde_fields = ["username", "password", "download_path", "proxy",
        ...               "date_mode", "date_from", "date_to", "last_n_days"]
        >>> overrides = load_overrides_from_airflow(gde_fields)
        >>> config = GDEConfig.from_yaml_config(env="prod", overrides=overrides)
    """
    overrides: Dict[str, Any] = {}

    # Cargar desde Connection
    if conn_id:
        try:
            from airflow.sdk.bases.hook import BaseHook

            conn = BaseHook.get_connection(conn_id)
            if conn.login:
                overrides["username"] = conn.login
            if conn.password:
                overrides["password"] = conn.password

            # Cargar extras del connection
            extras = getattr(conn, "extra_dejson", {}) or {}
            if isinstance(extras, dict):
                overrides.update(extras)

        except ImportError:
            logger.warning("⚠ Airflow no disponible, omitiendo carga desde Connection")
        except Exception as exc:
            logger.warning("⚠ No se pudo obtener la conexión '%s': %s", conn_id, exc)

    # Cargar desde Variables de Airflow (sobrescriben connection)
    if variable_prefix and fields:
        try:
            from airflow.sdk import Variable

            for field in fields:
                var_name = f"{variable_prefix}{field.upper()}"
                try:
                    value = Variable.get(var_name)
                    overrides[field] = value
                except KeyError:
                    continue
                except Exception as exc:
                    logger.debug("⚠ No se pudo leer la Variable '%s': %s", var_name, exc)

        except ImportError:
            logger.warning("⚠ Airflow no disponible, omitiendo carga desde Variables")

    logger.info("🧩 Overrides cargados desde Airflow: %s", sorted(overrides.keys()))
    return overrides
