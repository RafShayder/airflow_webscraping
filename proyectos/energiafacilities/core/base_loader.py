from __future__ import annotations
from typing import Any, Dict, Optional
from types import SimpleNamespace
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import re

logger = logging.getLogger(__name__)


def _validate_sql_identifier(identifier: str, name: str = "identifier") -> str:
    """
    Valida que un identificador SQL (schema, table, column) sea seguro.
    Solo permite letras, números, guiones bajos y punto (para schema.table).
    Previene SQL injection.
    
    Si el identificador empieza con número, lo escapa con comillas dobles
    para que PostgreSQL lo acepte (ej: "45_min_medir_voltaj_bb_que").
    """
    if not identifier:
        raise ValueError(f"{name} no puede estar vacío")

    # Si ya está escapado con comillas dobles, removerlas para validar
    is_quoted = identifier.startswith('"') and identifier.endswith('"')
    if is_quoted:
        identifier = identifier[1:-1]
    
    # Validar que no tenga comillas dobles dentro (previene SQL injection)
    if '"' in identifier:
        raise ValueError(f"{name} inválido: '{identifier}' contiene comillas dobles no permitidas")

    # Permitir schema.table con punto, pero validar cada parte
    if '.' in identifier:
        parts = identifier.split('.')
        if len(parts) > 2:
            raise ValueError(f"{name} inválido: demasiados puntos en '{identifier}'")
        validated_parts = []
        for part in parts:
            # Validar caracteres permitidos: letras, números, guiones bajos
            if not re.match(r'^[a-zA-Z0-9_]+$', part):
                raise ValueError(f"{name} inválido: '{part}' contiene caracteres no permitidos")
            # Si empieza con número, escapar con comillas
            if re.match(r'^[0-9]', part):
                validated_parts.append(f'"{part}"')
            else:
                validated_parts.append(part)
        return '.'.join(validated_parts)
    else:
        # Validar caracteres permitidos: letras, números, guiones bajos
        if not re.match(r'^[a-zA-Z0-9_]+$', identifier):
            raise ValueError(f"{name} inválido: '{identifier}' contiene caracteres no permitidos")
        # Si empieza con número, escapar con comillas dobles para PostgreSQL
        if re.match(r'^[0-9]', identifier):
            return f'"{identifier}"'
        return identifier

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
    def verificar_datos(self, data: Any, column_mapping: Optional[Dict[str, str]] = None, sheet_name: str = 0, strictreview=True, numerofilasalto: int =0, table_name:str =None):
        """Verifica columnas entre origen y tabla destino (mapeo invertido: DB ➜ Excel)."""
        try:
            logger.info("Verificando data y la db destino")
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
                logger.error("Formato no soportado (DataFrame, Excel o CSV)")
                raise ValueError("Formato no soportado. Debe ser DataFrame, Excel o CSV") 

            logger.debug(f"{origen} leído correctamente con {len(df.columns)} columnas.")

            # --- Mapeo invertido: destino ➜ origen ---
            if column_mapping:
                inverse_map = {v: k for k, v in column_mapping.items()}
                df = df.rename(columns=inverse_map)
                logger.debug("Mapeo de columnas aplicado (modo invertido DB ➜ Excel)")

            # Obtener nombres de columnas sin comillas para comparación
            # (las columnas del DataFrame pueden tener comillas si empiezan con números)
            columnas_origen = {col.strip('"') for col in df.columns}

            # --- Obtener columnas de la tabla destino ---
            # Usar pg_catalog para obtener nombres exactos (incluyendo columnas con comillas)
            # information_schema retorna nombres en minúsculas y sin comillas, pero pg_catalog
            # retorna el nombre exacto como está almacenado
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT a.attname 
                        FROM pg_catalog.pg_attribute a
                        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                        WHERE n.nspname = %s
                          AND c.relname = %s
                          AND a.attnum > 0
                          AND NOT a.attisdropped
                        ORDER BY a.attnum;
                    """, (self._cfgload.schema, table_name or self._cfgload.table))
                    # pg_catalog retorna el nombre exacto como está almacenado (puede tener comillas si se creó así)
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
        numerofilasalto:int =0,
        modo=None,
        table_name:str =None,
        schema: str = None,
        fecha_carga: Optional[datetime] = None
    ):
        """Carga datos a PostgreSQL (usa mapeo invertido DB ➜ Excel).
        Parámetros:
        - data: DataFrame o ruta a archivo Excel/CSV.
        - sheet_name: nombre o índice de hoja (si es Excel).
        - batch_size: tamaño de lote para inserción.
        - column_mapping: mapeo de columnas (DB ➜ Excel).
        - numerofilasalto: número de filas a saltar al leer el archivo.
        - modo: política de inserción ('replace', 'append', 'fail').
        - fecha_carga: fecha y hora de inicio del proceso de carga (opcional).
        """
        try:
            logger.info("Iniciando validación de carga")
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, str) and data.lower().endswith((".xlsx", ".xls")):
                df = pd.read_excel(data, sheet_name=sheet_name, skiprows=numerofilasalto)
            elif isinstance(data, str) and data.lower().endswith(".csv"):
                df = pd.read_csv(data, skiprows=numerofilasalto)
            else:
                logger.error("Formato no reconocido: debe ser DataFrame, Excel o CSV")
                raise ValueError("Formato no reconocido. Debe ser DataFrame, Excel o CSV") 

            # --- Mapeo inverso ---
            if column_mapping:
                inverse_map = {v: k for k, v in column_mapping.items()}
                cols_existentes = [c for c in inverse_map.keys() if c in df.columns]
                
                if cols_existentes:
                    df = df[cols_existentes].rename(columns=inverse_map)
                    logger.debug("Mapeo invertido aplicado (DB ➜ Excel)")
                else:
                    logger.debug("No se aplicó el mapeo: ninguna columna coincide con el DataFrame.")
                    raise ValueError("No se pudo aplicar el mapeo: ninguna columna coincide con el DataFrame")
        
            batch = batch_size or getattr(self._cfgload, "chunksize", 10000)
            logger.info(f"Iniciando carga: {len(df)} filas, {len(df.columns)} columnas")
            return self.insert_dataframe(df, batch_size=batch, modo=modo, table_name=table_name, schema=schema, fecha_carga=fecha_carga)

        except Exception as e:
            logger.error(f"Error al cargar los datos: {e}")
            raise

    # ----------
    # INSERCIÓN POR LOTES
    # ----------
    def insert_dataframe(self, df: pd.DataFrame, batch_size: int = 10000, modo: str = None, table_name: str =None, schema: str=None, fecha_carga: Optional[datetime] = None):
        if df.empty:
            msg = "DataFrame vacío, no hay datos para insertar"
            logger.error(msg)
            raise ValueError(msg)

        try:
            # Validar identificadores SQL para prevenir SQL injection
            validated_schema = _validate_sql_identifier(schema or self._cfgload.schema, "schema")
            validated_table = _validate_sql_identifier(table_name or self._cfgload.table, "table")

            # Obtener nombres exactos de columnas de la tabla (como están almacenados en PostgreSQL)
            # Esto es necesario para columnas que empiezan con números y están definidas con comillas
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT a.attname 
                        FROM pg_catalog.pg_attribute a
                        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                        WHERE n.nspname = %s
                          AND c.relname = %s
                          AND a.attnum > 0
                          AND NOT a.attisdropped
                        ORDER BY a.attnum;
                    """, (validated_schema, validated_table))
                    columnas_tabla_exactas_raw = [r[0] for r in cur.fetchall()]
                    # Crear diccionario: nombre sin comillas -> nombre exacto
                    columnas_tabla_exactas = {}
                    for col_exacta in columnas_tabla_exactas_raw:
                        col_sin_comillas = col_exacta.strip('"')
                        columnas_tabla_exactas[col_sin_comillas.lower()] = col_exacta
            
            logger.info(f"📊 Columnas encontradas en tabla '{validated_table}': {len(columnas_tabla_exactas)}")
            
            # Crear mapeo: nombre del DataFrame (sin comillas) -> nombre exacto en tabla
            # Solo incluir columnas que existan en la tabla
            mapeo_cols_df_a_tabla = {}
            cols_para_insert = []
            columnas_df_filtradas = []
            
            for col_df in df.columns:
                col_df_sin_comillas = col_df.strip('"')
                col_df_lower = col_df_sin_comillas.lower()
                
                # Buscar la columna en la tabla que coincida
                if col_df_lower in columnas_tabla_exactas:
                    col_tabla_exacta = columnas_tabla_exactas[col_df_lower]
                    mapeo_cols_df_a_tabla[col_df] = col_tabla_exacta
                    columnas_df_filtradas.append(col_df)
                    
                    # Construir nombre para INSERT: si empieza con número, escapar con comillas
                    col_tabla_sin_comillas = col_tabla_exacta.strip('"')
                    if re.match(r'^[0-9]', col_tabla_sin_comillas):
                        cols_para_insert.append(f'"{col_tabla_sin_comillas}"')
                    else:
                        cols_para_insert.append(col_tabla_sin_comillas)
                else:
                    # Columna no encontrada en tabla - omitirla (no intentar insertarla)
                    logger.warning(f"⚠️  Columna '{col_df_sin_comillas}' del DataFrame no existe en tabla '{validated_table}', será omitida")
            
            # Filtrar DataFrame para incluir solo columnas que existen en la tabla
            if len(columnas_df_filtradas) < len(df.columns):
                columnas_omitidas = set(df.columns) - set(columnas_df_filtradas)
                logger.warning(f"📋 Se omitirán {len(columnas_omitidas)} columnas que no existen en la tabla: {list(columnas_omitidas)[:5]}{'...' if len(columnas_omitidas) > 5 else ''}")
                df = df[columnas_df_filtradas]
            
            # Agregar columna fechacarga si se proporciona fecha_carga y la columna existe en la tabla
            if fecha_carga is not None:
                if 'fechacarga' in columnas_tabla_exactas:
                    # La columna fechacarga existe en la tabla
                    col_fechacarga_exacta = columnas_tabla_exactas['fechacarga']
                    df['fechacarga'] = fecha_carga
                    columnas_df_filtradas.append('fechacarga')
                    # Agregar fechacarga a las columnas para INSERT usando el nombre exacto
                    col_fechacarga_sin_comillas = col_fechacarga_exacta.strip('"')
                    if re.match(r'^[0-9]', col_fechacarga_sin_comillas):
                        cols_para_insert.append(f'"{col_fechacarga_sin_comillas}"')
                    else:
                        cols_para_insert.append(col_fechacarga_sin_comillas)
                    logger.info(f"📅 Columna 'fechacarga' agregada con valor: {fecha_carga}")
                else:
                    logger.warning(f"⚠️  Se proporcionó fecha_carga pero la columna 'fechacarga' no existe en la tabla '{validated_table}'")
            
            if not cols_para_insert:
                raise ValueError(f"No hay columnas válidas para insertar en la tabla '{validated_table}'")
            
            cols = ', '.join(cols_para_insert)

            full_table = f"{validated_schema}.{validated_table}"
            total_rows = len(df)
            modo =modo or getattr(self._cfgload, "if_exists", "replace").lower()

            with self._connect() as conn:
                with conn.cursor() as cur:
                    # Verificar existencia de la tabla
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = LOWER(%s)
                              AND table_name = LOWER(%s)
                        );
                    """, (validated_schema, validated_table))
                    tabla_existe = cur.fetchone()[0]

                    # Política de inserción
                    if modo == "replace" and tabla_existe:
                        cur.execute(f"TRUNCATE TABLE {full_table} RESTART IDENTITY CASCADE;")
                        conn.commit()
                    elif modo == "fail" and tabla_existe:
                        logger.error(f"La tabla {full_table} ya existe y if_exists='fail'")
                        raise ValueError(f"La tabla {full_table} ya existe y if_exists='fail'")


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
