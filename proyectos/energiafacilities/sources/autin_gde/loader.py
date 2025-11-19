from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional, Union

# Configurar path para imports cuando se ejecuta directamente
current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[3]))  # /.../proyectos
sys.path.insert(0, str(current_path.parents[4]))  # repo root for other imports

import os

from core import load_config
from core.base_loader import BaseLoaderPostgres
from core.helpers import traerjson

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def load_gde(filepath: Optional[PathLike] = None, env: str = None) -> dict:
    """Carga el archivo de GDE a PostgreSQL usando el mapeo definido en YAML/JSON."""
    # Usar el mismo mecanismo que el scraper: obtener configuración desde YAML
    if env is None:
        env = os.getenv("ENV_MODE", "dev")

    # Cargar configuración desde YAML (igual que hace el scraper)
    # load_config() ya obtiene la configuración de PostgreSQL desde la Connection postgres_siom_{env}
    config = load_config(env=env)

    # Obtener configuración de PostgreSQL desde la sección "postgress" del config
    # Esta sección ya está cargada desde la Connection de Airflow por load_config()
    postgres_config = config.get("postgress", {})

    logger.info(f"Configuración PostgreSQL obtenida: {list(postgres_config.keys()) if postgres_config else 'VACÍA'}")
    if postgres_config:
        logger.info("Campos encontrados: %s", {k: "***" if "pass" in k.lower() else v for k, v in postgres_config.items()})

    if not postgres_config:
        env_value = env or "dev"
        logger.error("No se encontró configuración PostgreSQL en Connection de Airflow")
        logger.error("Necesitas crear la Connection 'postgres_siom_%s' en Airflow (Admin > Connections):", env_value)
        logger.error("  - Conn Id: postgres_siom_%s", env_value)
        logger.error("  - Conn Type: postgres")
        logger.error("  - Host: dirección del servidor PostgreSQL")
        logger.error("  - Schema: nombre de la base de datos")
        logger.error("  - Login: usuario de PostgreSQL")
        logger.error("  - Password: contraseña del usuario")
        logger.error("  - Port: puerto del servidor PostgreSQL (ej: 5432)")
        raise ValueError(f"Configuración PostgreSQL no encontrada. Crea la Connection 'postgres_siom_{env_value}' en Airflow")

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
