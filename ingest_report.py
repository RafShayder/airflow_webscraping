"""
Permite cargar la data en PostgreSQL aplicando una política configurable:
    - append  : inserta registros adicionales
    - replace : borra contenido previo antes de insertar
    - skip    : no hace nada si la tabla ya contiene datos

El script intenta ser reutilizable admitiendo CSV y XLSX, detecta tipos básicos
de columnas y crea la tabla destino cuando no existe.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

from dotenv import load_dotenv

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover - se ejecuta sólo si falta pandas
    raise SystemExit(
        "Este script requiere pandas. Instálalo con `pip install pandas openpyxl`."
    ) from exc

try:
    import psycopg  # psycopg 3
except ImportError as exc:  # pragma: no cover - idem
    raise SystemExit(
        "Este script requiere psycopg (v3). Instálalo con `pip install psycopg[binary]`."
    ) from exc

logger = logging.getLogger(__name__)

DEFAULT_DOWNLOAD_ENV = "DOWNLOAD_PATH"
DEFAULT_FILENAME_ENV = "GDE_OUTPUT_FILENAME"
POLICY_ENV = "GDE_INGEST_POLICY"
TARGET_TABLE_ENV = "GDE_INGEST_TARGET_TABLE"
SOURCE_FILE_ENV = "GDE_INGEST_FILE"
CREATE_TABLE_ENV = "GDE_INGEST_CREATE_TABLE"

SUPPORTED_POLICIES = {"append", "replace", "skip"}
SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


@dataclass
class IngestionConfig:
    file_path: Path
    schema: str
    table: str
    policy: str
    create_table: bool


def load_dataframe(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Extensión no soportada: {suffix}. Usa CSV o XLSX.")

    logger.info("📂 Leyendo archivo: %s", path)
    if suffix == ".csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path)

    if df.empty:
        raise ValueError("El archivo no contiene filas.")

    return df


def normalize_column_names(columns: Sequence[str]) -> List[str]:
    """Normaliza nombres de columnas a snake_case ASCII y evita duplicados."""
    normalized = []
    counts: Counter[str] = Counter()

    for original in columns:
        value = unicodedata.normalize("NFKD", str(original))
        value = value.encode("ascii", "ignore").decode("ascii")
        value = re.sub(r"[^\w\s]", "_", value, flags=re.ASCII)
        value = re.sub(r"\s+", "_", value).strip("_").lower() or "col"
        counts[value] += 1
        if counts[value] > 1:
            value = f"{value}_{counts[value]}"
        normalized.append(value)

    return normalized


def infer_column_types(df: pd.DataFrame) -> List[str]:
    """Mapea dtypes de pandas a tipos de PostgreSQL sencillos."""
    types = []
    for series in df:
        dtype = df[series].dtype
        if pd.api.types.is_integer_dtype(dtype):
            types.append("BIGINT")
        elif pd.api.types.is_float_dtype(dtype):
            types.append("DOUBLE PRECISION")
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            types.append("TIMESTAMP")
        elif pd.api.types.is_bool_dtype(dtype):
            types.append("BOOLEAN")
        else:
            types.append("TEXT")
    return types


def split_table_identifier(identifier: str) -> Tuple[str, str]:
    if "." in identifier:
        schema, table = identifier.split(".", 1)
    else:
        schema, table = "public", identifier
    if not schema or not table:
        raise ValueError("El identificador de tabla es inválido.")
    return schema, table


def resolve_file_path(args: argparse.Namespace) -> Path:
    if args.file:
        return Path(args.file).expanduser().resolve()

    env_file = os.getenv(SOURCE_FILE_ENV)
    if env_file:
        return Path(env_file).expanduser().resolve()

    download_dir = Path(os.getenv(DEFAULT_DOWNLOAD_ENV, ".")).expanduser()
    explicit = os.getenv(DEFAULT_FILENAME_ENV)
    if explicit:
        candidate = (download_dir / explicit).resolve()
        if candidate.exists():
            return candidate

    if not download_dir.exists():
        raise FileNotFoundError("No se encontró archivo y la carpeta de descargas no existe.")

    candidates = sorted(
        (p for p in download_dir.iterdir() if p.suffix.lower() in SUPPORTED_EXTENSIONS and p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            "No se encontraron archivos CSV/XLSX en la carpeta de descargas. "
            "Proporciona un archivo explícitamente con --file o variable de entorno."
        )
    return candidates[0]


def load_config(args: argparse.Namespace) -> IngestionConfig:
    table_identifier = args.table or os.getenv(TARGET_TABLE_ENV, "raw.gde_export")
    schema, table = split_table_identifier(table_identifier)

    policy = (args.policy or os.getenv(POLICY_ENV, "append")).lower()
    if policy not in SUPPORTED_POLICIES:
        raise ValueError(f"Política de ingesta inválida: {policy}")

    create_table = args.create_table
    if create_table is None:
        create_table = os.getenv(CREATE_TABLE_ENV, "true").strip().lower() == "true"

    file_path = resolve_file_path(args)
    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo especificado: {file_path}")

    return IngestionConfig(
        file_path=file_path,
        schema=schema,
        table=table,
        policy=policy,
        create_table=create_table,
    )


def get_connection() -> psycopg.Connection:
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    missing = [k for k, v in [("DB_NAME", db_name), ("DB_USER", db_user), ("DB_PASSWORD", db_password)] if not v]
    if missing:
        raise RuntimeError(f"Faltan variables de entorno requeridas: {', '.join(missing)}")

    conn = psycopg.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
        autocommit=False,
    )
    return conn


def ensure_table(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    columns: Sequence[str],
    pg_types: Sequence[str],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        exists = cur.fetchone()[0] > 0

    if exists:
        logger.info("ℹ Tabla %s.%s ya existe, no se modificará.", schema, table)
        return

    definitions = ", ".join(f'"{col}" {col_type}' for col, col_type in zip(columns, pg_types))
    statement = f'CREATE TABLE "{schema}"."{table}" ({definitions});'
    logger.info("🛠️ Creando tabla %s.%s", schema, table)
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute(statement)
    conn.commit()


def table_has_rows(conn: psycopg.Connection, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(f'SELECT EXISTS (SELECT 1 FROM "{schema}"."{table}" LIMIT 1);')
        return cur.fetchone()[0]


def truncate_table(conn: psycopg.Connection, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE "{schema}"."{table}";')


def align_columns(
    conn: psycopg.Connection, schema: str, table: str, ingest_columns: Sequence[str]
) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        existing = [row[0] for row in cur.fetchall()]

    missing = [col for col in ingest_columns if col not in existing]
    if missing:
        raise RuntimeError(
            "Las siguientes columnas del archivo no existen en la tabla destino: "
            + ", ".join(missing)
        )

    return [col for col in existing if col in ingest_columns]


def copy_rows(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence],
) -> None:
    column_list = ", ".join(f'"{c}"' for c in columns)
    copy_sql = f'COPY "{schema}"."{table}" ({column_list}) FROM STDIN WITH (FORMAT CSV)'
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for row in rows:
                values = ["" if value is None else value for value in row]
                copy.write_row(values)


def dataframe_to_rows(df: pd.DataFrame, columns: Sequence[str]) -> Iterable[Sequence]:
    for _, record in df[columns].iterrows():
        yield tuple(record.where(pd.notna(record), None))


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingesta de reportes GDE a PostgreSQL")
    parser.add_argument("--file", help="Ruta al archivo exportado (CSV/XLSX)")
    parser.add_argument("--table", help="Tabla destino (schema.table). Por defecto raw.gde_export")
    parser.add_argument(
        "--policy",
        choices=sorted(SUPPORTED_POLICIES),
        help="Política de ingesta: append, replace o skip",
    )
    parser.add_argument(
        "--create-table",
        dest="create_table",
        action="store_true",
        help="Crea la tabla si no existe (default)",
    )
    parser.add_argument(
        "--no-create-table",
        dest="create_table",
        action="store_false",
        help="No intenta crear la tabla si no existe",
    )
    parser.set_defaults(create_table=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    load_dotenv()
    args = parse_args(argv or sys.argv[1:])
    cfg = load_config(args)

    df = load_dataframe(cfg.file_path)
    original_columns = list(df.columns)
    normalized_columns = normalize_column_names(original_columns)
    df.columns = normalized_columns
    pg_types = infer_column_types(df)

    logger.info("📊 Columnas detectadas: %s", ", ".join(normalized_columns))

    with get_connection() as conn:
        if cfg.create_table:
            ensure_table(conn, cfg.schema, cfg.table, normalized_columns, pg_types)

        if cfg.policy == "skip" and table_has_rows(conn, cfg.schema, cfg.table):
            logger.info("⏩ La tabla ya contiene datos y la política es 'skip'. No se realizaron cambios.")
            conn.rollback()
            return

        if cfg.policy == "replace":
            truncate_table(conn, cfg.schema, cfg.table)

        insert_columns = align_columns(conn, cfg.schema, cfg.table, normalized_columns)
        if not insert_columns:
            raise RuntimeError("No hay columnas en común entre el archivo y la tabla destino.")

        copy_rows(conn, cfg.schema, cfg.table, insert_columns, dataframe_to_rows(df, insert_columns))
        conn.commit()

    logger.info("✅ Ingesta completada: %s filas insertadas", len(df.index))


if __name__ == "__main__":  # pragma: no cover
    main()
