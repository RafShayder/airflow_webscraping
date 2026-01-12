from __future__ import annotations
import logging
import warnings
import re
from typing import Optional, Dict, Any
from types import SimpleNamespace
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

# Silenciar advertencias irrelevantes de pandas/SQLAlchemy
warnings.filterwarnings("ignore", category=UserWarning)

logger = logging.getLogger(__name__)


def _validate_sql_identifier(identifier: str, name: str = "identifier") -> str:
    """
    Valida que un identificador SQL (schema, table, column) sea seguro.
    Solo permite letras, números, guiones bajos y punto (para schema.table).
    Previene SQL injection.
    """
    if not identifier:
        raise ValueError(f"{name} no puede estar vacío")

    # Permitir schema.table con punto, pero validar cada parte
    if '.' in identifier:
        parts = identifier.split('.')
        if len(parts) > 2:
            raise ValueError(f"{name} inválido: demasiados puntos en '{identifier}'")
        for part in parts:
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', part):
                raise ValueError(f"{name} inválido: '{part}' contiene caracteres no permitidos")
    else:
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValueError(f"{name} inválido: '{identifier}' contiene caracteres no permitidos")

    return identifier


class PostgresConnector:
    """
    Clase de conexión y extracción de datos desde PostgreSQL.
    
     Compatible con:
      - pandas.read_sql_query()
      - SQLAlchemy Engine reutilizable
      - Ejecución de queries, funciones y procedimientos almacenados
      - ETLs (Airflow, n8n, etc.)

    Ejemplo:
        cfg = {
            "host": "localhost",
            "port": 5432,
            "database": "mi_db",
            "user": "admin",
            "password": "1234"
        }

        pg = PostgresConnector(cfg)
        pg.validar_conexion()

        df = pg.ejecutar("SELECT * FROM clientes WHERE pais = %s", ("PERU",))
        df2 = pg.extract({"schema": "public", "table": "ventas", "limit": 100})
    """

    def __init__(self, config: dict):
        if not isinstance(config, dict):
            logger.debug("El parámetro 'config' debe ser un dict con las claves esperadas.")
            raise ValueError("El parámetro 'config' debe ser un dict con las claves esperadas")
        self._cfg = SimpleNamespace(**config)
        self._engine: Optional[Engine] = None

    # ------------------------------------------------
    # CONEXIÓN (SQLAlchemy)
    # ------------------------------------------------
    def _connect(self) -> Engine:
        """Crea o reutiliza un engine SQLAlchemy."""
        if self._engine:
            return self._engine

        try:
            conn_url = URL.create(
                "postgresql+psycopg2",
                username=self._cfg.user,
                password=self._cfg.password,
                host=self._cfg.host,
                port=self._cfg.port,
                database=self._cfg.database,
            )
            self._engine = create_engine(conn_url, pool_pre_ping=True)
            logger.debug(f"Conexión establecida con {self._cfg.host}")
            return self._engine
        except Exception as e:
            logger.error(f"Error creando engine SQLAlchemy: {e}")
            raise

    def close(self):
        """Cierra el engine SQLAlchemy (si existe)."""
        if self._engine:
            self._engine.dispose()
            logger.debug("Conexión a PostgreSQL cerrada.")
            self._engine = None

    # ------------------------------------------------
    # VALIDAR CONEXIÓN
    # ------------------------------------------------
    def validar_conexion(self) -> Dict[str, Any]:
        """Verifica la conectividad básica con la base de datos."""
        try:
            with self._connect().connect() as conn:
                conn.execute(text("SELECT 1"))
            info = {"status": "success", "code": 200, "etl_msg": f"Conexión exitosa a {self._cfg.host}"}
            logger.debug(info["etl_msg"])
            return info
        except Exception as e:
            info = {"status": "error", "code": 401, "etl_msg": f"Error de conectividad: {e}"}
            logger.error(info["etl_msg"])
            raise

    # ------------------------------------------------
    # EJECUTAR CONSULTAS O FUNCIONES
    # ------------------------------------------------
    def ejecutar(
    self,
    consulta: str,
    parametros: Optional[tuple] = None,
    tipo: str = "query"
) -> pd.DataFrame:
        """
        Ejecuta una consulta SQL, función o procedimiento almacenado y retorna un DataFrame si hay resultados.
        tipo:
            - "query": SELECT u otras consultas que retornan filas
            - "fn"    : función que retorna un conjunto
            - "sp"    : procedimiento almacenado
        """
        tipo = tipo.lower().strip()
        engine = self._connect()
        conn = None
        cur = None

        try:
            conn = engine.raw_connection()
            cur = conn.cursor()

            if tipo == "fn":
                cur.callproc(consulta, parametros or ())

            elif tipo == "sp":
                if parametros:
                    placeholders = ", ".join(["%s"] * len(parametros))
                    cur.execute(f"CALL {consulta}({placeholders});", parametros)
                else:
                    cur.execute(f"CALL {consulta}();")
                conn.commit()

            else:  # query normal
                cur.execute(consulta, parametros)

            # Retornar DataFrame si hay resultados
            if cur.description:
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=cols)
            else:
                df = pd.DataFrame()

            logger.debug(f"Ejecución de {tipo} '{consulta}' completada correctamente.")
            return df

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error al ejecutar {tipo} '{consulta}': {e}")
            raise

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()


    # ------------------------------------------------
    # EXTRACCIÓN DE TABLAS (con SQL dinámico)
    # ------------------------------------------------
    def extract(self, config: dict) -> pd.DataFrame:
        """
        Extrae datos de una tabla PostgreSQL según parámetros.
        config:
            {
                "schema": "public",
                "table": "clientes",
                "columns": ["id", "nombre"] -> opcional,
                "where": "pais = 'PERU'" -> opcional,
                "limit": 1000 -> opcional,
                "batch_size": 5000 -> opcional
            }

        """
        if not isinstance(config, dict):
            raise ValueError("config debe ser un dict con los parámetros de extracción.")

        cfg = SimpleNamespace(**config)

        # Validar identificadores SQL para prevenir SQL injection
        schema = _validate_sql_identifier(cfg.schema, "schema")
        table = _validate_sql_identifier(cfg.table, "table")

        # Validar columnas si se proporcionan
        if getattr(cfg, "columns", None):
            validated_cols = [_validate_sql_identifier(col, f"columna '{col}'") for col in cfg.columns]
            cols = ", ".join(validated_cols)
        else:
            cols = "*"

        sql = f"SELECT {cols} FROM {schema}.{table}"
        if getattr(cfg, "where", None):
            sql += f" WHERE {cfg.where}"
        if getattr(cfg, "limit", None):
            sql += f" LIMIT {cfg.limit}"

        logger.debug(f"Ejecutando extracción: {sql}")
        engine = self._connect()

        try:
            df = pd.read_sql_query(text(sql), engine, chunksize=getattr(cfg, "batch_size", None))
            if isinstance(df, pd.io.parsers.TextFileReader):  # batch mode
                df = pd.concat(df, ignore_index=True)
            logger.debug(f"Extracción completada ({len(df)} filas).")
            return df
        except Exception as e:
            logger.error(f"Error durante la extracción: {e}")
            raise

    # ------------------------------------------------
    # CONTEXTO "with"
    # ------------------------------------------------
    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
