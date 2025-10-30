from __future__ import annotations
from typing import Any, Dict, Optional
from types import SimpleNamespace
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from core.exceptions import NonRetryableExtractError

class BaseLoaderPostgres:
    """
    Clase estandar de carga de datos
      - variables: config(parametros de conexión) y configload (parametros de carga)
      - soporta ->  Excel, CSV o DataFrame
      - variable data -> str| df 
      - Inserta por lotes (batch)
      - Permite mapeo opcional de columnas, sino carga toda la data tal cual
    """

    def __init__(self, config: dict, configload: dict):
        if not isinstance(config, dict):
            raise TypeError("config debe ser un dict con las claves esperadas")
        if not isinstance(configload, dict):
            raise TypeError("configload debe ser un dict con los parámetros de carga")

        self._cfg = SimpleNamespace(**config)
        self._cfgload = SimpleNamespace(**configload)

    # ----------
    #  CONEXIÓN
    # ----------
    def _connect(self):
        """Crea conexión a PostgreSQL"""
        try:
            return psycopg2.connect(
                host=self._cfg.host,
                port=self._cfg.port,
                dbname=self._cfg.database,
                user=self._cfg.user,
                password=self._cfg.password
            )
        except Exception as e:
            raise NonRetryableExtractError(f"Error al conectar a PostgreSQL: {e}")
    # ----------
    #  VALIDAR CONECTIVIDAD
    # ----------
    def validar_conexion(self):
        """Verifica si la conexión al host es exitosa"""
        try:
            self._connect().close()
            return {"status": "success", "code": 200, "message": f"Conexión exitosa a {self._cfg.host}"}
        except Exception as e:
            return {"status": "error", "code": 401, "message": f"Error de conectividad: {str(e)}"}

    # ----------
    #  VERIFICA LAS COLUMNAS DEL JSON
    # ----------
    def verificar_datos(self, data: Any, column_mapping: Optional[Dict[str, str]] = None, sheet_name: str = 0):
        """Verifica columnas entre origen (Excel/CSV/DF) y tabla destino"""
        try:
            # --- Obtener DataFrame origen
            if isinstance(data, pd.DataFrame):
                df = data
                origen = "DataFrame en memoria"
            elif isinstance(data, str) and data.lower().endswith((".xlsx", ".xls")):
                df = pd.read_excel(data, sheet_name=sheet_name)
                origen = f"Archivo Excel ({data})"
            elif isinstance(data, str) and data.lower().endswith(".csv"):
                df = pd.read_csv(data)
                origen = f"Archivo CSV ({data})"
            else:
                raise NonRetryableExtractError("Formato no soportado (debe ser DataFrame, Excel o CSV)")

            print(f" {origen} leído con {len(df.columns)} columnas.")

            # --- Aplicar mapeo si existe
            if column_mapping:
                df = df.rename(columns=column_mapping)
                print(" Mapeo de columnas aplicado.")

            columnas_origen = set(df.columns)

            # --- Obtener columnas de la tabla postgress destino
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns
                        WHERE table_schema =LOWER(%s) AND table_name = LOWER(%s)
                        ORDER BY ordinal_position;
                    """, (self._cfgload.schema, self._cfgload.table))
                    columnas_tabla = {r[0] for r in cur.fetchall()}
            
            # --- comparamos si falta algo (error) o sobra (alerta)  ---        
            sobrantes = columnas_origen - columnas_tabla
            faltantes = columnas_tabla - columnas_origen
       
            if faltantes:
                return {
                    "status": "error",
                    "code": 400,
                    "message": f"Columnas no encontradas en la tabla destino: {', '.join(faltantes)}"
                }

            print(" Verificación de columnas exitosa.")
            if sobrantes:
                print(f" Alerta: la tabla tiene columnas adicionales que no están en el origen: {', '.join(sobrantes)}")

            return {"status": "success", "code": 200, "message": "Columnas verificadas correctamente"}

        except Exception as e:
            raise NonRetryableExtractError(f"Error durante verificación de columnas: {e}")

    # ----------
    #  METODO DE CARGA DE DATOS
    # ----------
    def load_data(
        self,
        data: Any,
        sheet_name: str = 0,
        batch_size: Optional[int] = None,
        column_mapping: Optional[Dict[str, str]] = None
    ):
        """Carga datos desde DataFrame, Excel o CSV."""
        try:
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, str) and data.lower().endswith((".xlsx", ".xls")):
                df = pd.read_excel(data, sheet_name=sheet_name)
            elif isinstance(data, str) and data.lower().endswith(".csv"):
                df = pd.read_csv(data)
            else:
                raise NonRetryableExtractError("Formato de entrada no reconocido")

            if column_mapping:
                df = df.rename(columns=column_mapping)
                columnas_existentes = [c for c in column_mapping.keys() if c in df.columns]
                df = df[columnas_existentes].rename(columns=column_mapping)

            batch = batch_size or getattr(self._cfgload, "chunksize", 10000)

            print(f" Iniciando carga: {len(df)} filas, {len(df.columns)} columnas")
            return self.insert_dataframe(df, batch_size=batch)

        except Exception as e:
            raise NonRetryableExtractError(f"Error al cargar los datos: {e}")

    # ----------
    #  INSERTAMOS POR LOTES
    # ----------
    def insert_dataframe(self, df: pd.DataFrame, batch_size: int = 10000):
        #se considera 3 politicas('append', 'replace' o 'fail').

        # Validación inicial: DataFrame vacío
        if df.empty:
            return {
                "status": "error",
                "code": 204,
                "message": "DataFrame vacío, no hay datos para insertar"
            }

        try:
            cols = ', '.join(df.columns)
            full_table = f"{self._cfgload.schema}.{self._cfgload.table}"
            total_rows = len(df)
            modo = getattr(self._cfgload, "if_exists", "replace").lower()

            with self._connect() as conn:
                with conn.cursor() as cur:
                    # Verificar existencia de la tabla destino
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = LOWER(%s)
                            AND table_name = LOWER(%s)
                        );
                    """, (self._cfgload.schema, self._cfgload.table))
                    tabla_existe = cur.fetchone()[0]
                    
                    # Política de inserción: manejo de 'fail', 'replace', 'append'
                    if modo == "fail":
                        if tabla_existe:
                            # FAIL: tabla ya existe → no sobreescribir
                            return {
                                "status": "error",
                                "code": 409,
                                "message": f"La tabla {full_table} ya existe y la política if_exists='fail' impide sobreescribir."
                            }
                        else:
                            # FAIL: crear automáticamente según el DataFrame
                            columnas_sql = ', '.join([f'"{col}" TEXT' for col in df.columns])
                            create_sql = f'CREATE TABLE {full_table} ({columnas_sql});'
                            cur.execute(create_sql)
                            conn.commit()
                            print(f"Tabla {full_table} creada automáticamente (modo 'fail').")
                            
                    elif modo == "replace":
                        if tabla_existe:
                            cur.execute(f"TRUNCATE TABLE {full_table} RESTART IDENTITY CASCADE;")
                            conn.commit()

                    elif modo == "append":
                        # En modo APPEND, simplemente se agregan filas
                        pass

                    else:
                        return {
                            "status": "error",
                            "code": 400,
                            "message": f"Valor de if_exists no reconocido: '{modo}'. Usa 'append', 'replace' o 'fail'."
                        }

                    # Inserción por lotes
                    insert_sql = f"INSERT INTO {full_table} ({cols}) VALUES %s"

                    for start in range(0, total_rows, batch_size):
                        chunk = df.iloc[start:start + batch_size]
                        values = [tuple(x) for x in chunk.to_numpy()]
                        execute_values(cur, insert_sql, values)
                        conn.commit()

            return {
                "status": "success",
                "code": 200,
                "message": f"{total_rows} filas insertadas correctamente con modo '{modo}'"
            }

        # ----------
        #  MANEJO DE ERRORES
        # ----------
        except psycopg2.OperationalError as e:
            # Errores para reintento
            return {
                "status": "error",
                "code": 502,
                "message": f"Error operativo en PostgreSQL (reintentar): {e}"
            }

        except Exception as e:
            # Errores criticos: formato incorrecto, permisos, etc.
            return {
                "status": "error",
                "code": 500,
                "message": f"Error durante la inserción: {e}"
            }
