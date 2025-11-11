from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Union

from energiafacilities.core import load_config
from energiafacilities.core.base_loader import BaseLoaderPostgres
from energiafacilities.core.utils import traerjson

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def load_gde(filepath: Optional[PathLike] = None, env: str = None) -> dict:
    """
    Carga los datos extraídos de GDE hacia PostgreSQL.

    Args:
        filepath: Ruta al archivo Excel descargado. Si no se proporciona,
                 se usa la configuración del YAML
        env: Entorno (dev, prod). Si no se proporciona, usa ENV_MODE o 'dev'

    Returns:
        Diccionario con el resultado de la carga (status, code, etl_msg)

    Example:
        >>> resultado = load_gde("./tmp/Console_GDE_export.xlsx")
        >>> print(resultado['etl_msg'])
    """
    try:
        # Cargar configuraciones desde config_dev.yaml o config_prod.yaml
        config = load_config(env=env)
        postgres_config = config.get("postgress", {})  # Nota: en config_dev.yaml es "postgress" con doble 's'
        gde_config = config.get("gde", {})

        # Validar que existen las configuraciones necesarias
        if not postgres_config:
            raise ValueError("No se encontró configuración 'postgress' en config YAML")
        if not gde_config:
            raise ValueError("No se encontró configuración 'gde' en config YAML")

        # Crear instancia del loader
        loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=gde_config
        )

        # Validar conexión
        logger.info("Validando conexión a PostgreSQL...")
        loader.validar_conexion()

        # Cargar mapeo de columnas
        logger.info("Cargando mapeo de columnas...")
        columnas = traerjson(
            archivo='config/columnas/columns_map.json',
            valor='gde_tasks'
        )

        # Determinar ruta del archivo
        if not filepath:
            local_dir = gde_config.get('local_dir', './tmp')
            filename = gde_config.get('specific_filename', 'Console_GDE_export.xlsx')
            filepath = f"{local_dir}/{filename}"

        filepath_str = str(filepath)
        logger.info(f"Cargando datos desde: {filepath_str}")

        # Verificar datos (sin strictreview para permitir columnas faltantes)
        logger.info("Verificando estructura de datos...")
        loader.verificar_datos(
            data=filepath_str,
            column_mapping=columnas,
            strictreview=False,  # Permite columnas opcionales (fecha_ingesta, archivo_origen)
            numerofilasalto=0
        )

        # Cargar datos
        logger.info("Iniciando carga de datos a PostgreSQL...")
        resultado = loader.load_data(
            data=filepath_str,
            column_mapping=columnas,
            numerofilasalto=0
        )

        logger.info(f"Carga completada exitosamente: {resultado['etl_msg']}")
        return resultado

    except FileNotFoundError as e:
        msg = f"Archivo no encontrado: {e}"
        logger.error(msg)
        raise
    except ValueError as e:
        msg = f"Error de configuración: {e}"
        logger.error(msg)
        raise
    except Exception as e:
        msg = f"Error durante la carga de datos: {e}"
        logger.error(msg)
        raise
