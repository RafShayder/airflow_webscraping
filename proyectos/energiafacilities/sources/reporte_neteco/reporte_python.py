"""
Genera un reporte XLSX de faltantes de data por sitio (NetEco) usando pandas.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# Ensure local execution can resolve energiafacilities imports.
BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from core.base_exporters import FileExporter
from core.base_postgress import PostgresConnector
from core.utils import load_config, setup_logging


logger = logging.getLogger(__name__)

SQL_BASE_NETECO = """
SELECT
  site_name,
  fecha::date AS fecha
FROM ods.web_hd_neteco_diaria;
"""

REPORT_COLUMNS = [
    "codigo_sitio",
    "fecha_inicio",
    "fecha_fin",
    "dias_esperados",
    "dias_con_data",
    "dias_faltantes",
    "semana_reporte",
    "semana_inicio",
    "semana_fin",
    "dias_esperados_semana",
    "dias_con_data_semana",
    "dias_faltantes_semana",
    "riesgo_actual",
]

COLUMN_NAME_MAP = {
    "codigo_sitio": "Codigo Sitio",
    "fecha_inicio": "Primera Lectura",
    "fecha_fin": "Fecha Evaluacion",
    "dias_esperados": "Dias Esperados",
    "dias_con_data": "Dias con Data",
    "dias_faltantes": "Dias Faltantes",
    "semana_reporte": "Semana Reporte",
    "semana_inicio": "Semana Inicio",
    "semana_fin": "Semana Fin",
    "dias_esperados_semana": "Dias Esperados Semana - Ayer",
    "dias_con_data_semana": "Dias con Data Semana - Ayer",
    "dias_faltantes_semana": "Dias Faltantes Semana - Ayer",
    "riesgo_actual": "Riesgo Actual",
}


def get_report_dir(output_dir: Optional[str | Path] = None) -> Path:
    """Define el directorio base donde se guardan los reportes."""
    if output_dir:
        return Path(output_dir)
    airflow_home = os.environ.get("AIRFLOW_HOME")
    if airflow_home:
        return Path(airflow_home) / "tmp" / "neteco-reports"
    return BASE_DIR / "tmp" / "neteco-reports"


def _week_params(reference_date: date) -> tuple[pd.Timestamp, pd.Timestamp, str]:
    week_start_current = reference_date - timedelta(days=reference_date.weekday())
    semana_inicio = week_start_current - timedelta(days=7)
    semana_fin = reference_date - timedelta(days=1)
    iso_year, iso_week, _ = semana_inicio.isocalendar()
    semana_reporte = f"{iso_year}-{iso_week:02d}"
    return pd.Timestamp(semana_inicio), pd.Timestamp(semana_fin), semana_reporte


def build_report_dataframe(
    df_raw: pd.DataFrame,
    reference_date: Optional[date] = None,
) -> pd.DataFrame:
    """Construye el dataframe final del reporte en base a data cruda."""
    if df_raw.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    ref_date = reference_date or datetime.now().date()
    semana_inicio, semana_fin, semana_reporte = _week_params(ref_date)
    fecha_fin = pd.Timestamp(ref_date)

    df = df_raw.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.normalize()
    df["codigo_sitio"] = (
        df["site_name"]
        .astype("string")
        .str.strip()
        .str.split("_", n=1)
        .str[0]
    )

    df = df.dropna(subset=["codigo_sitio", "fecha"])

    rango_all = (
        df.groupby("codigo_sitio", dropna=True)["fecha"]
        .agg(fecha_inicio="min", dias_con_data="nunique")
        .reset_index()
    )
    rango_all["fecha_fin"] = fecha_fin
    rango_all["dias_esperados"] = (rango_all["fecha_fin"] - rango_all["fecha_inicio"]).dt.days + 1
    rango_all["dias_faltantes"] = rango_all["dias_esperados"] - rango_all["dias_con_data"]

    semana_mask = (df["fecha"] >= semana_inicio) & (df["fecha"] <= semana_fin)
    semana_data = (
        df.loc[semana_mask]
        .groupby("codigo_sitio", dropna=True)["fecha"]
        .nunique()
        .rename("dias_con_data_semana")
        .reset_index()
    )

    result = rango_all.merge(semana_data, on="codigo_sitio", how="left")
    dias_esperados_semana = (semana_fin - semana_inicio).days + 1
    result["semana_reporte"] = semana_reporte
    result["semana_inicio"] = semana_inicio
    result["semana_fin"] = semana_fin
    result["dias_esperados_semana"] = dias_esperados_semana
    result["dias_con_data_semana"] = result["dias_con_data_semana"].fillna(0).astype(int)
    result["dias_faltantes_semana"] = dias_esperados_semana - result["dias_con_data_semana"]

    result["riesgo_actual"] = np.select(
        [
            result["dias_faltantes_semana"] >= 7,
            result["dias_faltantes_semana"] >= 4,
            result["dias_faltantes_semana"] >= 2,
        ],
        ["ALTO", "MEDIO", "BAJO"],
        default="OK",
    )

    result = result[result["dias_faltantes_semana"] > 0]
    result = result.sort_values(
        ["dias_faltantes_semana", "codigo_sitio"],
        ascending=[False, True],
    )

    return result[REPORT_COLUMNS]


def generate_xlsx_report(df: pd.DataFrame, output_path: Path) -> None:
    """Genera el XLSX con tabla de datos."""
    exporter = FileExporter()
    exporter.export_dataframe(df, str(output_path), index=False)


def run_reporte_neteco_faltantes_python(
    env: Optional[str] = None,
    output_dir: Optional[str | Path] = None,
) -> str:
    """Ejecuta el query base y genera el reporte XLSX con pandas."""
    config = load_config(env)
    resolved_env = env.lower().strip() if env else getattr(config, "_env", "dev")
    postgres_config = config.get("postgress", {})
    if not postgres_config:
        raise ValueError("Configuracion de Postgres no encontrada en 'postgress'.")

    connector = PostgresConnector(postgres_config)
    try:
        connector.validar_conexion()
        logger.info("Extrayendo data base de NetEco para reporte con pandas")
        df_raw = connector.ejecutar(SQL_BASE_NETECO, tipo="query")
    finally:
        connector.close()

    df_report = build_report_dataframe(df_raw)
    df_report = df_report.rename(columns=COLUMN_NAME_MAP)

    output_dir = get_report_dir(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"neteco_faltantes_mes_{resolved_env}_{timestamp}_python.xlsx"

    generate_xlsx_report(df_report, output_path)

    logger.info("Reporte generado: %s", output_path)
    return str(output_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera reporte XLSX de faltantes NetEco usando pandas"
    )
    parser.add_argument("--env", default=None, help="Entorno a usar (dev, staging, prod)")
    parser.add_argument("--output-dir", default=None, help="Directorio de salida para el XLSX")
    parser.add_argument("--log-level", default="INFO", help="Nivel de log")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    setup_logging(args.log_level)
    output = run_reporte_neteco_faltantes_python(env=args.env, output_dir=args.output_dir)
    print(f"Reporte generado: {output}")


if __name__ == "__main__":
    main()
