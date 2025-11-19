from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, Union, Dict, List
from datetime import datetime

# Configurar path para imports cuando se ejecuta directamente
current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[3]))  # /.../proyectos
sys.path.insert(0, str(current_path.parents[4]))  # repo root for other imports

from core import load_config
from core.base_loader import BaseLoaderPostgres
from core.helpers import traerjson

PathLike = Union[str, Path]

# Mapeo de tablas SQL a pestañas Excel (11 tablas según fase2.sql)
TABLAS_DYNAMIC_CHECKLIST = {
    "cf_banco_de_baterias": "CF - BANCO DE BATERIAS",
    "cf_bastidor_distribucion": "CF - BASTIDOR DISTRIBUCION",
    "cf_cuadro_de_fuerza": "CF - CUADRO DE FUERZA",
    "cf_modulos_rectificadores": "CF - MODULOS RECTIFICADORES",
    "cf_tablero_ac_de_cuadro_de_fu": "CF - TABLERO AC DE CUADRO DE FU",
    "cf_descarga_controlada_bater": "CF_ - DESCARGA CONTROLADA BATER",
    "ie_datos_spat_general": "IE - DATOS SPAT GENERAL",
    "ie_mantenimiento_pozo_por_poz": "IE - MANTENIMIENTO POZO POR POZ",
    "ie_suministro_de_energia": "IE - SUMINISTRO DE ENERGÍA",
    "ie_tablero_principal": "IE - TABLERO PRINCIPAL",
    "ie_tablero_secundario": "IE - TABLERO SECUNDARIO",
}


def load_single_table(
    tabla_sql: str,
    nombre_pestana: str,
    filepath: PathLike,
    fecha_carga: Optional[datetime] = None,
    env: str = None,
) -> dict:
    """Carga una pestaña del Excel de Dynamic Checklist hacia su tabla correspondiente."""
    (
        loader,
        _dynamic_config,
        schema,
        load_mode,
    ) = _build_dynamic_loader(env=env)
    fecha_carga = fecha_carga or datetime.now()
    resultado = _process_table(
        loader=loader,
        tabla_sql=tabla_sql,
        nombre_pestana=nombre_pestana,
        filepath=str(filepath),
        schema=schema,
        load_mode=load_mode,
        fecha_carga=fecha_carga,
    )
    return {
        "status": "success",
        "code": 200,
        "etl_msg": resultado["resultado"].get(
            "etl_msg", f"Tabla {tabla_sql} cargada exitosamente"
        ),
        "tabla": tabla_sql,
        "pestana": nombre_pestana,
        "resultado": resultado["resultado"],
    }


def load_dynamic_checklist(filepath: Optional[PathLike] = None, env: str = None) -> dict:
    """
    Carga los datos extraídos de Dynamic Checklist hacia PostgreSQL.
    
    Procesa 11 pestañas del Excel y las carga en sus respectivas tablas:
    - Cada pestaña corresponde a una tabla en raw.*
    - Usa el mapeo de columnas desde columns_map_checklist.json
    - Carga todas las pestañas en orden

    Args:
        filepath: Ruta al archivo Excel descargado. Si no se proporciona,
                 se usa la configuración del YAML
        env: Entorno (dev, prod). Si no se proporciona, usa ENV_MODE o 'dev'

    Returns:
        Diccionario con el resultado de la carga (status, code, etl_msg)
        Incluye resumen de todas las tablas cargadas

    Example:
        >>> resultado = load_dynamic_checklist("./tmp/DynamicChecklist_SubPM.xlsx")
        >>> print(resultado['etl_msg'])
    """
    (
        loader,
        dynamic_config,
        schema,
        load_mode,
    ) = _build_dynamic_loader(env=env)

    if not filepath:
        local_dir = dynamic_config.get("local_dir", "./tmp")
        filename = dynamic_config.get("specific_filename", "DynamicChecklist_SubPM.xlsx")
        filepath = f"{local_dir}/{filename}"

    filepath_str = str(filepath)
    fecha_carga_inicio = datetime.now()
    resultados: List[Dict[str, object]] = []
    errores: List[Dict[str, object]] = []

    for tabla_sql, nombre_pestana in TABLAS_DYNAMIC_CHECKLIST.items():
        try:
            resultados.append(
                _process_table(
                    loader=loader,
                    tabla_sql=tabla_sql,
                    nombre_pestana=nombre_pestana,
                    filepath=filepath_str,
                    schema=schema,
                    load_mode=load_mode,
                    fecha_carga=fecha_carga_inicio,
                )
            )
        except Exception as exc:  # noqa: BLE001 - necesitamos capturar cualquier error por tabla
            errores.append(
                {
                    "tabla": tabla_sql,
                    "pestana": nombre_pestana,
                    "error": str(exc),
                    "tipo_error": type(exc).__name__,
                }
            )

    status = "success"
    code = 200
    msg = f"Todas las {len(resultados)} tablas cargadas exitosamente"
    if errores:
        status = "partial_success" if resultados else "error"
        code = 207 if resultados else 500
        msg = (
            f"Carga completada: {len(resultados)} exitosas,"
            f" {len(errores)} con errores"
        )

    return {
        "status": status,
        "code": code,
        "etl_msg": msg,
        "resultados": resultados,
        "errores": errores,
    }


def _build_dynamic_loader(
    env: Optional[str],
) -> tuple[BaseLoaderPostgres, Dict[str, object], str, str]:
    config = load_config(env=env)
    postgres_config = config.get("postgress", {})
    dynamic_checklist_config = config.get("dynamic_checklist", {})

    loader = BaseLoaderPostgres(
        config=postgres_config,
        configload=dynamic_checklist_config,
    )
    loader.validar_conexion()

    schema = dynamic_checklist_config.get("schema", "raw")
    load_mode = dynamic_checklist_config.get("load_mode", "append")
    return loader, dynamic_checklist_config, schema, load_mode


def _process_table(
    loader: BaseLoaderPostgres,
    tabla_sql: str,
    nombre_pestana: str,
    filepath: str,
    schema: str,
    load_mode: str,
    fecha_carga: datetime,
) -> Dict[str, object]:
    columnas = None
    try:
        columnas = traerjson(
            archivo="config/columnas/columns_map_checklist.json",
            valor=tabla_sql,
        )
    except Exception:
        columnas = None

    loader.verificar_datos(
        data=filepath,
        column_mapping=columnas,
        sheet_name=nombre_pestana,
        strictreview=False,
        numerofilasalto=0,
        table_name=tabla_sql,
    )

    resultado = loader.load_data(
        data=filepath,
        sheet_name=nombre_pestana,
        column_mapping=columnas,
        numerofilasalto=0,
        table_name=tabla_sql,
        schema=schema,
        modo=load_mode,
        fecha_carga=fecha_carga,
    )

    return {
        "tabla": tabla_sql,
        "pestana": nombre_pestana,
        "resultado": resultado,
    }


# Ejecución local (desarrollo/testing)
# Para producción, usar los DAGs de Airflow en dags/DAG_dynamic_checklist.py
# El entorno se determina automáticamente desde ENV_MODE o usa "dev" por defecto
if __name__ == "__main__":
    load_dynamic_checklist()
