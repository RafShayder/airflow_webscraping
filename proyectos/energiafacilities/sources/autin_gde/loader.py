from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional, Union

# Configurar path para imports cuando se ejecuta directamente
current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[3]))  # /.../proyectos
sys.path.insert(0, str(current_path.parents[4]))  # repo root for other imports

from energiafacilities.core import load_config
from energiafacilities.core.base_loader import BaseLoaderPostgres
from energiafacilities.core.utils import traerjson, _load_airflow_variables

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def load_gde(filepath: Optional[PathLike] = None, env: str = None) -> dict:
    """Carga el archivo de GDE a PostgreSQL usando el mapeo definido en YAML/JSON."""
    # Usar el mismo mecanismo que el scraper: obtener configuración desde YAML
    if env is None:
        env = os.getenv("ENV_MODE")

    # Cargar configuración desde YAML (igual que hace el scraper)
    config = load_config(env=env)

    # Obtener configuración de PostgreSQL desde Variables de Airflow
    # Primero intentar con los nombres estándar
    postgres_fields = ["database", "host", "port", "user", "password", "schema"]
    logger.info("Buscando configuración PostgreSQL en Variables de Airflow con prefijo 'POSTGRES_'...")

    postgres_config = _load_airflow_variables("POSTGRES_", "postgress", env or "dev")

    # Si faltan campos, intentar con nombres alternativos que están definidos
    if not postgres_config.get("database"):
        # Intentar POSTGRES_DB_DEV en lugar de POSTGRES_DATABASE_DEV
        try:
            from airflow.sdk import Variable
            db_var = f"POSTGRES_DB_{env.upper()}" if env else "POSTGRES_DB"
            postgres_config["database"] = Variable.get(db_var)
            logger.info(f"Database obtenido de Variable alternativa: {db_var}")
        except:
            pass

    if not postgres_config.get("password"):
        # Intentar POSTGRES_PASS_DEV en lugar de POSTGRES_PASSWORD_DEV
        try:
            from airflow.sdk import Variable
            pass_var = f"POSTGRES_PASS_{env.upper()}" if env else "POSTGRES_PASS"
            postgres_config["password"] = Variable.get(pass_var)
            logger.info(f"Password obtenido de Variable alternativa: {pass_var}")
        except:
            pass

    logger.info(f"Configuración PostgreSQL obtenida: {list(postgres_config.keys()) if postgres_config else 'VACÍA'}")
    if postgres_config:
        logger.info("Campos encontrados: %s", {k: "***" if "pass" in k.lower() else v for k, v in postgres_config.items()})

    if not postgres_config:
        logger.error("No se encontró configuración PostgreSQL en Variables de Airflow")
        logger.error("Necesitas definir las siguientes Variables en Airflow (Admin > Variables):")
        logger.error("  POSTGRES_HOST_DEV: dirección del servidor PostgreSQL (o POSTGRES_HOST)")
        logger.error("  POSTGRES_PORT_DEV: puerto del servidor PostgreSQL (ej: 5432) (o POSTGRES_PORT)")
        logger.error("  POSTGRES_DATABASE_DEV: nombre de la base de datos (o POSTGRES_DATABASE)")
        logger.error("  POSTGRES_USER_DEV: usuario de PostgreSQL (o POSTGRES_USER)")
        logger.error("  POSTGRES_PASSWORD_DEV: contraseña del usuario (o POSTGRES_PASSWORD)")
        logger.error("  POSTGRES_SCHEMA_DEV: esquema de la base de datos (opcional) (o POSTGRES_SCHEMA)")
        logger.error("")
        logger.error("Nota: Las variables con sufijo '_DEV' tienen prioridad sobre las genéricas")
        raise ValueError("Configuración PostgreSQL no encontrada. Define las Variables POSTGRES_*_DEV en Airflow")

    # Validar configuración PostgreSQL
    required_fields = ["host", "port", "database", "user", "password"]
    missing_fields = [field for field in required_fields if not postgres_config.get(field)]
    if missing_fields:
        raise ValueError(f"Configuración PostgreSQL incompleta. Faltan campos: {missing_fields}")

    logger.info("Configuración PostgreSQL validada: host=%s, database=%s",
                postgres_config.get("host"), postgres_config.get("database"))

    # Obtener configuración GDE
    gde_config = config.get("gde", {})

    loader = BaseLoaderPostgres(config=postgres_config, configload=gde_config)
    loader.validar_conexion()

    columnas = traerjson(
        archivo="config/columnas/columns_map_gde.json",
        valor="gde_tasks",
    )

    if not filepath:
        local_dir = gde_config.get("local_dir", "./tmp")
        filename = gde_config.get("specific_filename", "Console_GDE_export.xlsx")
        filepath = f"{local_dir}/{filename}"

    filepath_str = str(filepath)
    loader.verificar_datos(
        data=filepath_str,
        column_mapping=columnas,
        strictreview=False,
        numerofilasalto=0,
    )

    return loader.load_data(
        data=filepath_str,
        column_mapping=columnas,
        numerofilasalto=0,
    )


# Ejecución local (desarrollo/testing)
# Para producción, usar los DAGs de Airflow en dags/DAG_gde.py
# El entorno se determina automáticamente desde ENV_MODE o usa "dev" por defecto
if __name__ == "__main__":
    load_gde()
