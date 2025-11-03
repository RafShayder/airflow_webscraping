from __future__ import annotations
from typing import Any, Dict, Optional
from types import SimpleNamespace
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)

class BaseLoaderPostgres:
    """
    Clase estándar de carga de datos en PostgreSQL.
    - config: credenciales de conexión.
    - configload: parámetros de carga (schema, table, if_exists, etc.).
    - Soporta carga desde Excel, CSV o DataFrame.
    - Permite mapeo de columnas (de BD ➜ Excel).
    """

    def __init__(self, config: dict, configload: dict):
        if not isinstance(config, dict):
            logger.error("config debe ser un dict con las claves esperadas")
            raise ValueError("config debe ser un dict válido")
        if not isinstance(configload, dict):
            logger.error("configload debe ser un dict con los parámetros de carga")
            raise ValueError("configload debe ser un dict válido")

        self._cfg = SimpleNamespace(**config)
        self._cfgload = SimpleNamespace(**configload)

    # ----------
    # CONEXIÓN
    # ----------
    def _connect(self):
        logger.info("Verificando conectividad a PostgreSQL...")
        return psycopg2.connect(
            host=self._cfg.host,
            port=self._cfg.port,
            dbname=self._cfg.database,
            user=self._cfg.user,
            password=self._cfg.password
        )

    # ----------
    # VALIDAR CONEXIÓN
    # ----------
    def validar_conexion(self):
        try:
            self._connect().close()
            retornoinfo = {"status": "success", "code": 200, "etl_msg": f"Conexión exitosa a {self._cfg.host}"}
            logger.info("Conexión exitosa a PostgreSQL", extra=retornoinfo)
            return retornoinfo
        except Exception as e:
            retornoinfo = {"status": "error", "code": 401, "etl_msg": f"Error de conectividad: {str(e)}"}
            logger.error(f"Error de conectividad: {e}", extra=retornoinfo)
            raise

    # ----------
    # VERIFICAR DATOS Y MAPEAR COLUMNAS
    # ----------
    def verificar_datos(self, data: Any, column_mapping: Optional[Dict[str, str]] = None, sheet_name: str = 0, strictreview=True, numerofilasalto: int =0):
        """Verifica columnas entre origen y tabla destino (mapeo invertido: DB ➜ Excel)."""
        try:
            # --- Leer DataFrame ---
            if isinstance(data, pd.DataFrame):
                df = data
                origen = "DataFrame"
            elif isinstance(data, str) and data.lower().endswith((".xlsx", ".xls")):
                df = pd.read_excel(data, sheet_name=sheet_name,skiprows=numerofilasalto)
                origen = f"Excel ({data})"
            elif isinstance(data, str) and data.lower().endswith(".csv"):
                df = pd.read_csv(data,skiprows=numerofilasalto)
                origen = f"CSV ({data})"
            else:
                raise ValueError("Formato no soportado (DataFrame, Excel o CSV)")

            logger.info(f"{origen} leído correctamente con {len(df.columns)} columnas.")

            # --- Mapeo invertido: destino ➜ origen ---
            if column_mapping:
                inverse_map = {v: k for k, v in column_mapping.items()}
                df = df.rename(columns=inverse_map)
                logger.info("Mapeo de columnas aplicado (modo invertido DB ➜ Excel)")

            columnas_origen = set(df.columns)

            # --- Obtener columnas de la tabla destino ---
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = LOWER(%s)
                          AND table_name = LOWER(%s)
                        ORDER BY ordinal_position;
                    """, (self._cfgload.schema, self._cfgload.table))
                    columnas_tabla = {r[0] for r in cur.fetchall()}

            sobrantes = columnas_origen - columnas_tabla
            faltantes = columnas_tabla - columnas_origen


            if faltantes and strictreview:
                msg = f"Columnas no encontradas en origen: {', '.join(faltantes)}"
                logger.error(msg)
                raise ValueError(msg)
            elif faltantes and not strictreview:
                msg = f"Columnas no encontradas en origen: {', '.join(faltantes)}"
                logger.warning(msg)


            if sobrantes:
                logger.warning(f"Columnas adicionales en origen: {', '.join(sobrantes)}")

            retornoinfo = {"status": "success", "code": 200, "etl_msg": "Columnas verificadas correctamente"}
            logger.info("Verificación de columnas completada", extra=retornoinfo)
            return retornoinfo

        except Exception as e:
            logger.error(f"Error en verificación de columnas: {e}")
            raise

    # ----------
    # CARGA DE DATOS
    # ----------
    def load_data(
        self,
        data: Any,
        sheet_name: str = 0,
        batch_size: Optional[int] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        numerofilasalto:int =0
    ):
        """Carga datos a PostgreSQL (usa mapeo invertido DB ➜ Excel)."""
        try:

            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, str) and data.lower().endswith((".xlsx", ".xls")):
                df = pd.read_excel(data, sheet_name=sheet_name, skiprows=numerofilasalto)
            elif isinstance(data, str) and data.lower().endswith(".csv"):
                df = pd.read_csv(data, skiprows=numerofilasalto)
            else:
                raise ValueError("Formato no reconocido: debe ser DataFrame, Excel o CSV")

            # --- Mapeo inverso ---
            if column_mapping:
                inverse_map = {v: k for k, v in column_mapping.items()}
                cols_existentes = [c for c in inverse_map.keys() if c in df.columns]

                if cols_existentes:
                    df = df[cols_existentes].rename(columns=inverse_map)
                    logger.info("Mapeo invertido aplicado (DB ➜ Excel)")
                else:
                    logger.error("No se aplicó el mapeo: ninguna columna coincide con el DataFrame.")
                    raise

            batch = batch_size or getattr(self._cfgload, "chunksize", 10000)
            logger.info(f"Iniciando carga: {len(df)} filas, {len(df.columns)} columnas")
            return self.insert_dataframe(df, batch_size=batch)

        except Exception as e:
            logger.error(f"Error al cargar los datos: {e}")
            raise

    # ----------
    # INSERCIÓN POR LOTES
    # ----------
    def insert_dataframe(self, df: pd.DataFrame, batch_size: int = 10000):
        if df.empty:
            msg = "DataFrame vacío, no hay datos para insertar"
            logger.error(msg)
            raise ValueError(msg)

        try:
            cols = ', '.join(df.columns)
            full_table = f"{self._cfgload.schema}.{self._cfgload.table}"
            total_rows = len(df)
            modo = getattr(self._cfgload, "if_exists", "replace").lower()

            with self._connect() as conn:
                with conn.cursor() as cur:
                    # Verificar existencia de la tabla
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = LOWER(%s)
                              AND table_name = LOWER(%s)
                        );
                    """, (self._cfgload.schema, self._cfgload.table))
                    tabla_existe = cur.fetchone()[0]

                    # Política de inserción
                    if modo == "replace" and tabla_existe:
                        cur.execute(f"TRUNCATE TABLE {full_table} RESTART IDENTITY CASCADE;")
                        conn.commit()
                    elif modo == "fail" and tabla_existe:
                        logger.error(f"La tabla {full_table} ya existe y if_exists='fail'")
                        raise


                    insert_sql = f"INSERT INTO {full_table} ({cols}) VALUES %s"
                    for start in range(0, total_rows, batch_size):
                        chunk = df.iloc[start:start + batch_size]
                        values = [tuple(x) for x in chunk.to_numpy()]
                        execute_values(cur, insert_sql, values)
                        conn.commit()

            logger.info(f"{total_rows} filas insertadas correctamente (modo '{modo}')")
            return {"status": "success", "code": 200, "etl_msg": f"{total_rows} filas insertadas correctamente"}

        except Exception as e:
            logger.error(f"Error durante la inserción: {e}")
            raise
