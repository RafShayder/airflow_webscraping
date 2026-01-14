"""
Genera un reporte XLSX de anomalias (altas) en consumo diario de energia NetEco.
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

from core.base_exporters import FileExporter
from core.base_postgress import PostgresConnector
from core.utils import load_config, setup_logging


logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS = 30  # Dias recientes usados para calcular la mediana/MAD por sitio.
DEFAULT_Z_THRESHOLD = 3.5  # Umbral de z-score robusto para marcar anomalia alta.
DEFAULT_RECENT_DAYS = 7  # Dias recientes a reportar desde la ultima fecha disponible.
DEFAULT_NORMAL_STREAK = 3  # Registros normales consecutivos que cierran la alerta.
RECENT_COL_PREFIX = "consumo_dia_"  # Prefijo para columnas (1 = mas antiguo, N = mas reciente).

SQL_BASE_NETECO = """
SELECT
  site_name,
  fecha::date AS fecha,
  energy_consumption_per_day_kwh
FROM ods.web_hd_neteco_diaria
WHERE energy_consumption_per_day_kwh IS NOT NULL;
"""

COLUMN_NAME_MAP = {
    "site_name": "Site Name",
    "fecha": "Fecha",
    "energy_consumption_per_day_kwh": "Consumo Diario kWh",
    "median_30d": "Mediana 30d",
    "mad_30d": "MAD 30d",
    "z_score": "Z Score",
    "window_start": "Ventana Inicio",
    "window_end": "Ventana Fin",
    "window_samples": "Muestras Ventana",
}


def get_report_dir(output_dir: Optional[str | Path] = None) -> Path:
    """Define el directorio base donde se guardan los reportes."""
    if output_dir:
        return Path(output_dir)
    airflow_home = os.environ.get("AIRFLOW_HOME")
    if airflow_home:
        return Path(airflow_home) / "tmp" / "neteco-reports"
    return BASE_DIR / "tmp" / "neteco-reports"


def build_anomaly_dataframe(
    df_raw: pd.DataFrame,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    z_threshold: float = DEFAULT_Z_THRESHOLD,
    recent_days: int = DEFAULT_RECENT_DAYS,
    normal_streak: int = DEFAULT_NORMAL_STREAK,
) -> pd.DataFrame:
    """Construye el dataframe final con anomalias altas por site_name."""
    if df_raw.empty:
        return pd.DataFrame(columns=list(COLUMN_NAME_MAP.values()))

    df = df_raw.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.normalize()
    df["energy_consumption_per_day_kwh"] = pd.to_numeric(
        df["energy_consumption_per_day_kwh"], errors="coerce"
    )
    df = df.dropna(subset=["site_name", "fecha", "energy_consumption_per_day_kwh"])

    max_dates = df.groupby("site_name")["fecha"].max().rename("window_end")
    df = df.join(max_dates, on="site_name")
    df["window_start"] = df["window_end"] - pd.to_timedelta(window_days - 1, unit="D")

    df_window = df[df["fecha"] >= df["window_start"]].copy()
    if df_window.empty:
        return pd.DataFrame(columns=list(COLUMN_NAME_MAP.values()))

    df_window_all = df_window.copy()

    median = (
        df_window_all.groupby("site_name")["energy_consumption_per_day_kwh"]
        .median()
        .rename("median_30d")
    )
    df_window_all = df_window_all.join(median, on="site_name")
    df_window_all["abs_dev"] = (
        df_window_all["energy_consumption_per_day_kwh"] - df_window_all["median_30d"]
    ).abs()
    mad = df_window_all.groupby("site_name")["abs_dev"].median().rename("mad_30d")
    df_window_all = df_window_all.join(mad, on="site_name")

    mad_positive = df_window_all["mad_30d"] > 0
    df_window_all["z_score"] = pd.NA
    df_window_all.loc[mad_positive, "z_score"] = (
        0.6745
        * (
            df_window_all.loc[mad_positive, "energy_consumption_per_day_kwh"]
            - df_window_all.loc[mad_positive, "median_30d"]
        )
        / df_window_all.loc[mad_positive, "mad_30d"]
    )

    df_window_all["is_anomalia"] = False
    df_window_all.loc[mad_positive, "is_anomalia"] = (
        df_window_all.loc[mad_positive, "z_score"] >= z_threshold
    )
    df_window_all.loc[~mad_positive, "is_anomalia"] = (
        df_window_all.loc[~mad_positive, "energy_consumption_per_day_kwh"]
        > df_window_all.loc[~mad_positive, "median_30d"]
    )
    samples = (
        df_window_all.groupby("site_name")["energy_consumption_per_day_kwh"]
        .size()
        .rename("window_samples")
    )
    df_window = df_window_all[
        df_window_all["energy_consumption_per_day_kwh"] > df_window_all["median_30d"]
    ]

    anomalies = df_window[df_window["is_anomalia"]].copy()
    if anomalies.empty:
        return pd.DataFrame(columns=list(COLUMN_NAME_MAP.values()))

    active_sites = set()
    if normal_streak <= 0:
        active_sites = set(anomalies["site_name"].unique())
    else:
        last_anom = (
            df_window_all[df_window_all["is_anomalia"]]
            .groupby("site_name")["fecha"]
            .max()
        )
        if not last_anom.empty:
            after_last = df_window_all.join(last_anom.rename("last_anom"), on="site_name")
            after_last = after_last[after_last["fecha"] > after_last["last_anom"]]
            counts_after = after_last.groupby("site_name")["fecha"].size()
            active_sites = {
                site
                for site, last_date in last_anom.items()
                if counts_after.get(site, 0) < normal_streak
            }

    if active_sites:
        anomalies = anomalies[anomalies["site_name"].isin(active_sites)]
    else:
        return pd.DataFrame(columns=list(COLUMN_NAME_MAP.values()))

    if recent_days > 0:
        recent_cutoff = anomalies["window_end"] - pd.to_timedelta(recent_days - 1, unit="D")
        anomalies = anomalies[anomalies["fecha"] >= recent_cutoff]
        if anomalies.empty:
            return pd.DataFrame(columns=list(COLUMN_NAME_MAP.values()))

    anomalies = anomalies.join(samples, on="site_name")

    report = anomalies[
        [
            "site_name",
            "fecha",
            "energy_consumption_per_day_kwh",
            "median_30d",
            "mad_30d",
            "z_score",
            "window_start",
            "window_end",
            "window_samples",
        ]
    ].sort_values(["site_name", "fecha"], ascending=[True, True])

    report = report.rename(columns=COLUMN_NAME_MAP)

    if recent_days > 0:
        recent_values = df_window_all[
            df_window_all["fecha"] >= df_window_all["window_end"] - pd.to_timedelta(recent_days - 1, unit="D")
        ].assign(
            offset_days=lambda data: (
                (data["fecha"] - (data["window_end"] - pd.to_timedelta(recent_days - 1, unit="D")))
                .dt.days.astype("int")
            )
        )
        recent_values = recent_values[recent_values["offset_days"].between(0, recent_days - 1)]
        recent_values = recent_values.drop_duplicates(subset=["site_name", "offset_days"])
        recent_pivot = recent_values.pivot(
            index="site_name",
            columns="offset_days",
            values="energy_consumption_per_day_kwh",
        ).rename(columns=lambda c: f"{RECENT_COL_PREFIX}{int(c) + 1}")
        report = report.merge(recent_pivot, left_on="Site Name", right_index=True, how="left")

    return report


def generate_xlsx_report(df: pd.DataFrame, output_path: Path) -> None:
    """Genera el XLSX con la data de anomalias."""
    exporter = FileExporter()
    exporter.export_dataframe(df, str(output_path), index=False)


def run_reporte_anomalias_consumo(
    env: Optional[str] = None,
    output_dir: Optional[str | Path] = None,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    z_threshold: float = DEFAULT_Z_THRESHOLD,
    recent_days: int = DEFAULT_RECENT_DAYS,
    normal_streak: int = DEFAULT_NORMAL_STREAK,
) -> str:
    """Ejecuta el query base y genera el reporte XLSX con anomalias."""
    config = load_config(env)
    resolved_env = env.lower().strip() if env else getattr(config, "_env", "dev")
    postgres_config = config.get("postgress", {})
    if not postgres_config:
        logger.error("Configuracion de Postgres no encontrada en 'postgress'")
        raise ValueError("Configuracion de Postgres no encontrada en 'postgress'.")

    connector = PostgresConnector(postgres_config)
    try:
        connector.validar_conexion()
        logger.info("Extrayendo consumo NetEco para reporte de anomalias")
        df_raw = connector.ejecutar(SQL_BASE_NETECO, tipo="query")
    finally:
        connector.close()

    df_report = build_anomaly_dataframe(
        df_raw,
        window_days=window_days,
        z_threshold=z_threshold,
        recent_days=recent_days,
        normal_streak=normal_streak,
    )

    output_dir = get_report_dir(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"neteco_anomalias_consumo_{resolved_env}_{timestamp}.xlsx"

    generate_xlsx_report(df_report, output_path)

    logger.info("Reporte generado: %s", output_path)
    return str(output_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera reporte XLSX de anomalias (altas) de consumo NetEco"
    )
    parser.add_argument("--env", default=None, help="Entorno a usar (dev, staging, prod)")
    parser.add_argument("--output-dir", default=None, help="Directorio de salida para el XLSX")
    parser.add_argument(
        "--window-days",
        type=int,
        default=DEFAULT_WINDOW_DAYS,
        help="Dias usados para calcular la mediana/MAD por sitio",
    )
    parser.add_argument(
        "--z-threshold",
        type=float,
        default=DEFAULT_Z_THRESHOLD,
        help="Umbral de z-score robusto para marcar anomalia alta",
    )
    parser.add_argument(
        "--recent-days",
        type=int,
        default=DEFAULT_RECENT_DAYS,
        help="Dias recientes a reportar desde la ultima fecha del sitio",
    )
    parser.add_argument(
        "--normal-streak",
        type=int,
        default=DEFAULT_NORMAL_STREAK,
        help="Registros normales consecutivos que cierran la alerta",
    )
    parser.add_argument("--log-level", default="INFO", help="Nivel de log")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    setup_logging(args.log_level)
    output = run_reporte_anomalias_consumo(
        env=args.env,
        output_dir=args.output_dir,
        window_days=args.window_days,
        z_threshold=args.z_threshold,
        recent_days=args.recent_days,
        normal_streak=args.normal_streak,
    )
    print(f"Reporte generado: {output}")


if __name__ == "__main__":
    main()
