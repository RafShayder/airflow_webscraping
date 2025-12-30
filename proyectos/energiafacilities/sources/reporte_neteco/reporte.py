"""
Genera un reporte XLSX de faltantes de data por sitio (NetEco).
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# Ensure local execution can resolve energiafacilities imports.
BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from core.base_postgress import PostgresConnector
from core.utils import load_config, setup_logging


logger = logging.getLogger(__name__)

SQL_FALTANTES_NETECO = """
WITH base AS (
  SELECT
    split_part(trim(site_name), '_', 1) AS codigo_sitio,
    fecha::date AS fecha
  FROM ods.web_hd_neteco_diaria
),
 
-- Universal: MIN(fecha) por sitio y fecha_fin = HOY
rango_all AS (
  SELECT
    codigo_sitio,
    MIN(fecha) AS fecha_inicio,
    CURRENT_DATE AS fecha_fin,
    COUNT(DISTINCT fecha) AS dias_con_data
  FROM base
  GROUP BY codigo_sitio
),
 
-- Semana pasada COMPLETA (Lun-Dom) respecto a HOY
-- En Postgres, date_trunc('week', x) = lunes de esa semana
params AS (
  SELECT
    (date_trunc('week', CURRENT_DATE)::date - 7) AS semana_inicio, -- lunes semana pasada
    (date_trunc('week', CURRENT_DATE)::date - 1) AS semana_fin,    -- domingo semana pasada
    to_char((date_trunc('week', CURRENT_DATE)::date - 7), 'IYYY-IW') AS semana_reporte
),
 
-- Días con data por sitio en la semana pasada
semana_data AS (
  SELECT
    b.codigo_sitio,
    COUNT(DISTINCT b.fecha) AS dias_con_data_semana
  FROM base b
  CROSS JOIN params p
  WHERE b.fecha BETWEEN p.semana_inicio AND p.semana_fin
  GROUP BY b.codigo_sitio
)
 
SELECT
  r.codigo_sitio,

  -- Universal (hasta HOY)
  r.fecha_inicio,
  r.fecha_fin,
  (r.fecha_fin - r.fecha_inicio + 1) AS dias_esperados,
  r.dias_con_data,
  ((r.fecha_fin - r.fecha_inicio + 1) - r.dias_con_data) AS dias_faltantes,

  -- Semana pasada (Lun-Dom)
  p.semana_reporte,
  p.semana_inicio,
  p.semana_fin,
  7 AS dias_esperados_semana,
  COALESCE(s.dias_con_data_semana, 0) AS dias_con_data_semana,
  (7 - COALESCE(s.dias_con_data_semana, 0)) AS dias_faltantes_semana,

  -- Riesgo SOLO por faltantes de semana pasada
  CASE
    WHEN (7 - COALESCE(s.dias_con_data_semana, 0)) >= 7 THEN 'ALTO'
    WHEN (7 - COALESCE(s.dias_con_data_semana, 0)) >= 4 THEN 'MEDIO'
    WHEN (7 - COALESCE(s.dias_con_data_semana, 0)) >= 2 THEN 'BAJO'
    ELSE 'OK'
  END AS riesgo_actual

FROM rango_all r
CROSS JOIN params p
LEFT JOIN semana_data s
  ON s.codigo_sitio = r.codigo_sitio

-- Solo los que tienen faltantes en la semana pasada
WHERE (7 - COALESCE(s.dias_con_data_semana, 0)) > 0
ORDER BY dias_faltantes_semana DESC, r.codigo_sitio;
"""


def get_report_dir(output_dir: Optional[str | Path] = None) -> Path:
    """Define el directorio base donde se guardan los reportes."""
    if output_dir:
        return Path(output_dir)
    airflow_home = os.environ.get("AIRFLOW_HOME")
    if airflow_home:
        return Path(airflow_home) / "tmp" / "neteco-reports"
    return BASE_DIR / "tmp" / "neteco-reports"


def generate_xlsx_report(df: pd.DataFrame, output_path: Path) -> None:
    """Genera el XLSX con tabla de datos."""
    sheet_name = "faltantes_mes_actual"
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)


def run_reporte_neteco_faltantes(
    env: Optional[str] = None,
    output_dir: Optional[str | Path] = None,
) -> str:
    """Ejecuta el query y genera el reporte XLSX."""
    config = load_config(env)
    resolved_env = env.lower().strip() if env else getattr(config, "_env", "dev")
    postgres_config = config.get("postgress", {})
    if not postgres_config:
        raise ValueError("Configuracion de Postgres no encontrada en 'postgress'.")

    connector = PostgresConnector(postgres_config)
    try:
        connector.validar_conexion()
        logger.info("Ejecutando query de faltantes NetEco")
        df = connector.ejecutar(SQL_FALTANTES_NETECO, tipo="query")
    finally:
        connector.close()

    output_dir = get_report_dir(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"neteco_faltantes_mes_{resolved_env}_{timestamp}.xlsx"

    generate_xlsx_report(df, output_path)

    logger.info("Reporte generado: %s", output_path)
    return str(output_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Genera reporte XLSX de faltantes NetEco")
    parser.add_argument("--env", default=None, help="Entorno a usar (dev, staging, prod)")
    parser.add_argument("--output-dir", default=None, help="Directorio de salida para el XLSX")
    parser.add_argument("--log-level", default="INFO", help="Nivel de log")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    setup_logging(args.log_level)
    output = run_reporte_neteco_faltantes(env=args.env, output_dir=args.output_dir)
    print(f"Reporte generado: {output}")


if __name__ == "__main__":
    main()
