from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, Union

# Configurar path para imports cuando se ejecuta directamente
current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[3]))  # /.../proyectos
sys.path.insert(0, str(current_path.parents[4]))  # repo root for other imports

from energiafacilities.core import load_config
from energiafacilities.core.base_loader import BaseLoaderPostgres
from energiafacilities.core.utils import traerjson

PathLike = Union[str, Path]


def load_gde(filepath: Optional[PathLike] = None, env: str = None) -> dict:
    """Carga el archivo de GDE a PostgreSQL usando el mapeo definido en YAML/JSON."""
    config = load_config(env=env)
    postgres_config = config.get("postgress", {})
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
