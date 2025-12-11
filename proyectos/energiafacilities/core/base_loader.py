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


# =============================================================================
# FUNCIONES DE VALIDACIÓN SQL
# =============================================================================

def _strip_quotes(identifier: str) -> str:
    """Remueve comillas dobles del inicio y fin de un identificador si las tiene."""
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1]
    return identifier


def _validate_identifier_chars(identifier: str, name: str = "identifier") -> None:
    """
    Valida que un identificador solo contenga caracteres seguros.
    
    Args:
        identifier: El identificador a validar (sin comillas)
        name: Nombre descriptivo para mensajes de error
        
    Raises:
        ValueError: Si contiene caracteres no permitidos o comillas internas
    """
    if '"' in identifier:
        raise ValueError(f"{name} inválido: '{identifier}' contiene comillas dobles no permitidas")
    
    if not re.match(r'^[a-zA-Z0-9_]+$', identifier):
        raise ValueError(f"{name} inválido: '{identifier}' contiene caracteres no permitidos")


def _escape_identifier_if_needed(identifier: str) -> str:
    """
    Escapa un identificador con comillas dobles si empieza con número.
    PostgreSQL requiere comillas para identificadores que empiezan con número.
    
    Args:
        identifier: El identificador a escapar (ya validado)
        
    Returns:
        El identificador escapado si es necesario, o sin cambios
    """
    if re.match(r'^[0-9]', identifier):
        return f'"{identifier}"'
    return identifier


def _validate_sql_identifier(identifier: str, name: str = "identifier") -> str:
    """
    Valida que un identificador SQL (schema, table, column) sea seguro.
    Solo permite letras, números, guiones bajos y punto (para schema.table).
    Previene SQL injection.
    
    Si el identificador empieza con número, lo escapa con comillas dobles
    para que PostgreSQL lo acepte (ej: "45_min_medir_voltaj_bb_que").
    
    Args:
        identifier: El identificador SQL a validar
        name: Nombre descriptivo para mensajes de error
        
    Returns:
        El identificador validado y escapado si es necesario
        
    Raises:
        ValueError: Si el identificador es inválido
    """
    if not identifier:
        raise ValueError(f"{name} no puede estar vacío")

    # Remover comillas si ya las tiene
    identifier = _strip_quotes(identifier)

    # Manejar schema.table (con punto)
    if '.' in identifier:
        parts = identifier.split('.')
        if len(parts) > 2:
            raise ValueError(f"{name} inválido: demasiados puntos en '{identifier}'")
        
        validated_parts = []
        for part in parts:
            _validate_identifier_chars(part, name)
            validated_parts.append(_escape_identifier_if_needed(part))
        return '.'.join(validated_parts)
    
    # Identificador simple (sin punto)
    _validate_identifier_chars(identifier, name)
    return _escape_identifier_if_needed(identifier)

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
        logger.debug("Verificando conectividad a PostgreSQL...")
        return psycopg2.connect(
            host=self._cfg.host,
            port=self._cfg.port,
            dbname=self._cfg.database,
            user=self._cfg.user,
            password=self._cfg.password
        )

    # ----------
    # MÉTODOS PRIVADOS DE UTILIDAD
    # ----------
    def _read_data_source(
        self, 
        data: Any, 
        sheet_name: str = 0, 
        skiprows: int = 0
    ) -> tuple[pd.DataFrame, str]:
        """
        Lee datos desde DataFrame, Excel o CSV.
        
        Args:
            data: DataFrame o ruta a archivo Excel/CSV
            sheet_name: Nombre o índice de hoja (si es Excel)
            skiprows: Número de filas a saltar al leer
            
        Returns:
            Tupla de (DataFrame, descripción del origen)
            
        Raises:
            ValueError: Si el formato no es soportado o hay error de lectura
        """
        if isinstance(data, pd.DataFrame):
            return data, "DataFrame"
        
        if isinstance(data, str) and data.lower().endswith((".xlsx", ".xls")):
            try:
                df = pd.read_excel(data, sheet_name=sheet_name, skiprows=skiprows)
                return df, f"Excel ({data})"
            except ValueError as e:
                if "Worksheet named" in str(e) or "not found" in str(e).lower():
                    try:
                        xl_file = pd.ExcelFile(data, engine='openpyxl')
                        available_sheets = xl_file.sheet_names
                        error_msg = (
                            f"No se encontró la hoja '{sheet_name}' en el archivo '{data}'. "
                            f"Hojas disponibles: {', '.join([f'\"{s}\"' for s in available_sheets])}"
                        )
                        logger.error(error_msg)
                        raise ValueError(error_msg) from e
                    except ValueError:
                        raise
                    except Exception:
                        raise e
                raise
        
        if isinstance(data, str) and data.lower().endswith(".csv"):
            df = pd.read_csv(data, skiprows=skiprows)
            return df, f"CSV ({data})"
        
        logger.error("Formato no soportado (DataFrame, Excel o CSV)")
        raise ValueError("Formato no soportado. Debe ser DataFrame, Excel o CSV")

    def _apply_column_mapping(
        self, 
        df: pd.DataFrame, 
        column_mapping: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Aplica mapeo de columnas al DataFrame (mapeo invertido: DB ➜ Excel).
        Soporta coincidencia case-insensitive y sin espacios extra.
        
        Args:
            df: DataFrame con las columnas originales
            column_mapping: Mapeo de columnas (DB ➜ Excel)
            
        Returns:
            DataFrame con columnas renombradas y filtradas según el mapeo
            
        Raises:
            ValueError: Si ninguna columna coincide con el mapeo
        """
        # Invertir mapeo: Excel ➜ DB
        inverse_map = {v: k for k, v in column_mapping.items()}
        
        # Normalizar nombres de columnas para comparación
        df_cols_normalized = {col.strip(): col for col in df.columns}
        inverse_map_normalized = {k.strip(): v for k, v in inverse_map.items()}
        
        cols_existentes = []
        cols_mapeo = {}
        
        for excel_col_normalized, excel_col_original in df_cols_normalized.items():
            # Intentar coincidencia exacta primero
            if excel_col_normalized in inverse_map_normalized:
                cols_existentes.append(excel_col_original)
                cols_mapeo[excel_col_original] = inverse_map_normalized[excel_col_normalized]
            else:
                # Intentar búsqueda case-insensitive
                excel_col_lower = excel_col_normalized.lower()
                for map_key_normalized, map_value in inverse_map_normalized.items():
                    if map_key_normalized.lower() == excel_col_lower:
                        cols_existentes.append(excel_col_original)
                        cols_mapeo[excel_col_original] = map_value
                        logger.debug(f"Mapeo case-insensitive: '{excel_col_original}' -> '{map_value}'")
                        break
        
        if not cols_existentes:
            logger.debug("No se aplicó el mapeo: ninguna columna coincide con el DataFrame.")
            raise ValueError("No se pudo aplicar el mapeo: ninguna columna coincide con el DataFrame")
        
        # Aplicar mapeo y filtrar columnas
        columnas_originales = list(df.columns)
        df_mapped = df[cols_existentes].rename(columns=cols_mapeo)
        
        logger.debug(f"Mapeo aplicado: {len(cols_existentes)} columnas mapeadas")
        if len(cols_existentes) < len(columnas_originales):
            cols_no_mapeadas = set(columnas_originales) - set(cols_existentes)
            logger.debug(f"{len(cols_no_mapeadas)} columnas del Excel no están en el mapeo y serán omitidas")
        
        return df_mapped

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
    def verificar_datos(
        self, 
        data: Any, 
        column_mapping: Optional[Dict[str, str]] = None, 
        sheet_name: str = 0, 
        strictreview: bool = True, 
        numerofilasalto: int = 0, 
        table_name: str = None
    ):
        """Verifica columnas entre origen y tabla destino (mapeo invertido: DB ➜ Excel)."""
        try:
            logger.debug("Verificando data y la db destino")
            
            # Leer datos desde la fuente
            df, origen = self._read_data_source(data, sheet_name, numerofilasalto)
            logger.debug(f"{origen} leído correctamente con {len(df.columns)} columnas.")

            # Aplicar mapeo si se proporciona (mapeo invertido simple para verificación)
            if column_mapping:
                inverse_map = {v: k for k, v in column_mapping.items()}
                df = df.rename(columns=inverse_map)
                logger.debug("Mapeo de columnas aplicado")

            # Obtener columnas del origen (sin comillas)
            columnas_origen = {col.strip('"') for col in df.columns}

            # Obtener columnas de la tabla destino
            columnas_tabla = self._get_table_columns(table_name or self._cfgload.table)

            # Comparar columnas
            sobrantes = columnas_origen - columnas_tabla
            faltantes = columnas_tabla - columnas_origen
            
            if faltantes and strictreview:
                msg = f"Columnas no encontradas en origen: {', '.join(faltantes)}"
                logger.error(msg)
                raise ValueError(msg)
            elif faltantes and not strictreview:
                logger.debug(f"Columnas no encontradas en origen: {', '.join(faltantes)}")

            if sobrantes:
                logger.debug(f"Columnas adicionales en origen: {', '.join(sobrantes)}")

            retornoinfo = {"status": "success", "code": 200, "etl_msg": "Columnas verificadas correctamente"}
            logger.debug("Verificación de columnas completada", extra=retornoinfo)
            return retornoinfo

        except Exception as e:
            logger.error(f"Error en verificación de columnas: {e}")
            raise
    
    def _get_table_columns(self, table_name: str, schema: str = None) -> set:
        """
        Obtiene los nombres de columnas de una tabla PostgreSQL.
        
        Args:
            table_name: Nombre de la tabla
            schema: Schema de la tabla (opcional, usa el del config por defecto)
            
        Returns:
            Set con los nombres de las columnas
        """
        schema = schema or self._cfgload.schema
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT a.attname 
                    FROM pg_catalog.pg_attribute a
                    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                    WHERE LOWER(n.nspname) = LOWER(%s)
                      AND LOWER(c.relname) = LOWER(%s)
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY a.attnum;
                """, (schema, table_name))
                return {r[0] for r in cur.fetchall()}

    def _get_table_columns_exact(self, schema: str, table_name: str) -> Dict[str, str]:
        """
        Obtiene un diccionario de columnas: nombre_lowercase -> nombre_exacto.
        Útil para columnas que empiezan con números y están definidas con comillas.
        
        Args:
            schema: Schema de la tabla
            table_name: Nombre de la tabla
            
        Returns:
            Dict con {nombre_lowercased: nombre_exacto}
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT a.attname 
                    FROM pg_catalog.pg_attribute a
                    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                    WHERE LOWER(n.nspname) = LOWER(%s)
                      AND LOWER(c.relname) = LOWER(%s)
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY a.attnum;
                """, (schema, table_name))
                columnas_raw = [r[0] for r in cur.fetchall()]
                
        return {col.strip('"').lower(): col for col in columnas_raw}

    def _map_df_to_table_columns(
        self, 
        df: pd.DataFrame, 
        columnas_tabla: Dict[str, str],
        table_name: str
    ) -> tuple[pd.DataFrame, list]:
        """
        Mapea columnas del DataFrame a las columnas de la tabla PostgreSQL.
        Solo incluye columnas que existen en ambos.
        
        Args:
            df: DataFrame a procesar
            columnas_tabla: Dict {nombre_lowercase: nombre_exacto} de la tabla
            table_name: Nombre de la tabla (para logging)
            
        Returns:
            Tupla de (DataFrame filtrado, lista de columnas para INSERT)
        """
        cols_para_insert = []
        columnas_df_filtradas = []
        
        for col_df in df.columns:
            col_df_sin_comillas = col_df.strip('"')
            col_df_lower = col_df_sin_comillas.lower()
            
            if col_df_lower in columnas_tabla:
                col_tabla_exacta = columnas_tabla[col_df_lower]
                columnas_df_filtradas.append(col_df)
                
                # Escapar con comillas si empieza con número
                col_tabla_sin_comillas = col_tabla_exacta.strip('"')
                cols_para_insert.append(_escape_identifier_if_needed(col_tabla_sin_comillas))
            else:
                logger.debug(f"Columna '{col_df_sin_comillas}' del DataFrame no existe en tabla '{table_name}', será omitida")
        
        # Filtrar DataFrame
        if len(columnas_df_filtradas) < len(df.columns):
            columnas_omitidas = set(df.columns) - set(columnas_df_filtradas)
            logger.debug(f"Se omitirán {len(columnas_omitidas)} columnas que no existen en la tabla: {list(columnas_omitidas)[:5]}{'...' if len(columnas_omitidas) > 5 else ''}")
            df = df[columnas_df_filtradas]
        
        return df, cols_para_insert

    # ----------
    # CARGA DE DATOS
    # ----------
    def load_data(
        self,
        data: Any,
        sheet_name: str = 0,
        batch_size: Optional[int] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        numerofilasalto: int = 0,
        modo: str = None,
        table_name: str = None,
        schema: str = None,
        fecha_carga: Optional[datetime] = None
    ):
        """Carga datos a PostgreSQL (usa mapeo invertido DB ➜ Excel).
        
        Parámetros:
            data: DataFrame o ruta a archivo Excel/CSV.
            sheet_name: nombre o índice de hoja (si es Excel).
            batch_size: tamaño de lote para inserción.
            column_mapping: mapeo de columnas (DB ➜ Excel).
            numerofilasalto: número de filas a saltar al leer el archivo.
            modo: política de inserción ('replace', 'append', 'fail').
            table_name: nombre de la tabla destino (opcional).
            schema: nombre del schema destino (opcional).
            fecha_carga: fecha y hora de inicio del proceso de carga (opcional).
        """
        try:
            logger.debug("Iniciando validación de carga")
            
            # Leer datos desde la fuente
            df, _ = self._read_data_source(data, sheet_name, numerofilasalto)

            # Aplicar mapeo de columnas si se proporciona
            if column_mapping:
                df = self._apply_column_mapping(df, column_mapping)
        
            batch = batch_size or getattr(self._cfgload, "chunksize", 10000)
            logger.info(f"Iniciando carga: {len(df)} filas, {len(df.columns)} columnas")
            return self.insert_dataframe(
                df, 
                batch_size=batch, 
                modo=modo, 
                table_name=table_name, 
                schema=schema, 
                fecha_carga=fecha_carga
            )

        except Exception as e:
            logger.error(f"Error al cargar los datos: {e}")
            raise

    # ----------
    # INSERCIÓN POR LOTES
    # ----------
    def insert_dataframe(
        self, 
        df: pd.DataFrame, 
        batch_size: int = 10000, 
        modo: str = None, 
        table_name: str = None, 
        schema: str = None, 
        fecha_carga: Optional[datetime] = None
    ):
        """
        Inserta un DataFrame en una tabla PostgreSQL por lotes.
        
        Args:
            df: DataFrame a insertar
            batch_size: Tamaño del lote para inserción
            modo: Política de inserción ('replace', 'append', 'fail')
            table_name: Nombre de la tabla destino
            schema: Schema de la tabla destino
            fecha_carga: Fecha de carga a agregar (si la columna existe)
        """
        if df.empty:
            msg = "DataFrame vacío, no hay datos para insertar"
            logger.error(msg)
            raise ValueError(msg)

        try:
            # Validar identificadores SQL
            validated_schema = _validate_sql_identifier(schema or self._cfgload.schema, "schema")
            validated_table = _validate_sql_identifier(table_name or self._cfgload.table, "table")

            # Obtener columnas de la tabla
            columnas_tabla = self._get_table_columns_exact(validated_schema, validated_table)
            
            logger.debug(f"Columnas encontradas en tabla '{validated_table}': {len(columnas_tabla)}")
            logger.debug(f"Columnas del DataFrame: {list(df.columns)[:10]}{'...' if len(df.columns) > 10 else ''}")
            
            # Mapear columnas del DataFrame a la tabla
            df, cols_para_insert = self._map_df_to_table_columns(df, columnas_tabla, validated_table)
            
            # Agregar columna fechacarga si corresponde
            df, cols_para_insert = self._add_fecha_carga_column(
                df, cols_para_insert, columnas_tabla, fecha_carga, validated_table
            )
            
            if not cols_para_insert:
                logger.error(f"Columnas del DataFrame: {list(df.columns)}")
                logger.error(f"Columnas en la tabla: {list(columnas_tabla.keys())}")
                raise ValueError(
                    f"No hay columnas válidas para insertar en la tabla '{validated_table}'. "
                    f"Columnas del DataFrame: {list(df.columns)[:5]}. "
                    f"Columnas en tabla: {list(columnas_tabla.keys())[:5]}"
                )
            
            # Ejecutar inserción
            return self._execute_insert(
                df, 
                cols_para_insert, 
                validated_schema, 
                validated_table, 
                batch_size, 
                modo
            )

        except Exception as e:
            logger.error(f"Error durante la inserción: {e}")
            raise

    def _add_fecha_carga_column(
        self,
        df: pd.DataFrame,
        cols_para_insert: list,
        columnas_tabla: Dict[str, str],
        fecha_carga: Optional[datetime],
        table_name: str
    ) -> tuple[pd.DataFrame, list]:
        """
        Agrega la columna fechacarga al DataFrame si existe en la tabla.
        
        Args:
            df: DataFrame a modificar
            cols_para_insert: Lista de columnas para INSERT
            columnas_tabla: Dict de columnas de la tabla
            fecha_carga: Valor de fecha a agregar
            table_name: Nombre de la tabla (para logging)
            
        Returns:
            Tupla de (DataFrame modificado, lista de columnas actualizada)
        """
        if fecha_carga is None:
            return df, cols_para_insert
            
        if 'fechacarga' in columnas_tabla:
            col_fechacarga_exacta = columnas_tabla['fechacarga']
            df = df.copy()
            df['fechacarga'] = fecha_carga
            
            col_fechacarga_sin_comillas = col_fechacarga_exacta.strip('"')
            cols_para_insert = cols_para_insert + [_escape_identifier_if_needed(col_fechacarga_sin_comillas)]
            logger.debug(f"Columna 'fechacarga' agregada con valor: {fecha_carga}")
        else:
            logger.debug(f"Se proporcionó fecha_carga pero la columna 'fechacarga' no existe en la tabla '{table_name}'")
        
        return df, cols_para_insert

    def _execute_insert(
        self,
        df: pd.DataFrame,
        cols_para_insert: list,
        schema: str,
        table_name: str,
        batch_size: int,
        modo: str
    ) -> Dict[str, Any]:
        """
        Ejecuta la inserción de datos en PostgreSQL.
        
        Args:
            df: DataFrame con los datos a insertar
            cols_para_insert: Lista de nombres de columnas para INSERT
            schema: Schema de la tabla
            table_name: Nombre de la tabla
            batch_size: Tamaño del lote
            modo: Política de inserción
            
        Returns:
            Dict con el resultado de la operación
        """
        cols = ', '.join(cols_para_insert)
        full_table = f"{schema}.{table_name}"
        total_rows = len(df)
        modo = modo or getattr(self._cfgload, "if_exists", "replace").lower()

        with self._connect() as conn:
            with conn.cursor() as cur:
                # Verificar existencia de la tabla
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = LOWER(%s)
                          AND table_name = LOWER(%s)
                    );
                """, (schema, table_name))
                tabla_existe = cur.fetchone()[0]

                # Aplicar política de inserción
                if modo == "replace" and tabla_existe:
                    cur.execute(f"TRUNCATE TABLE {full_table} RESTART IDENTITY CASCADE;")
                    conn.commit()
                elif modo == "fail" and tabla_existe:
                    logger.error(f"La tabla {full_table} ya existe y if_exists='fail'")
                    raise ValueError(f"La tabla {full_table} ya existe y if_exists='fail'")

                # Insertar por lotes
                insert_sql = f"INSERT INTO {full_table} ({cols}) VALUES %s"
                for start in range(0, total_rows, batch_size):
                    chunk = df.iloc[start:start + batch_size]
                    values = [tuple(x) for x in chunk.to_numpy()]
                    execute_values(cur, insert_sql, values)
                    conn.commit()

        logger.info(f"{total_rows} filas insertadas correctamente (modo '{modo}')")
        return {"status": "success", "code": 200, "etl_msg": f"{total_rows} filas insertadas correctamente"}
