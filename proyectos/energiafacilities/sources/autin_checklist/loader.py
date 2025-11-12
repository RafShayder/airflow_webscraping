from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Union, Dict, List
from datetime import datetime

from energiafacilities.core import load_config
from energiafacilities.core.base_loader import BaseLoaderPostgres
from energiafacilities.core.utils import traerjson

logger = logging.getLogger(__name__)

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
    env: str = None
) -> dict:
    """
    Carga una sola tabla de Dynamic Checklist hacia PostgreSQL.
    
    Args:
        tabla_sql: Nombre de la tabla SQL destino (ej: "cf_banco_de_baterias")
        nombre_pestana: Nombre de la pestaña en el Excel (ej: "CF - BANCO DE BATERIAS")
        filepath: Ruta al archivo Excel
        fecha_carga: Fecha y hora de inicio del proceso (opcional, se usa datetime.now() si no se proporciona)
        env: Entorno (dev, prod). Si no se proporciona, usa ENV_MODE o 'dev'
    
    Returns:
        Diccionario con el resultado de la carga (status, code, etl_msg)
    
    Example:
        >>> resultado = load_single_table(
        ...     "cf_banco_de_baterias",
        ...     "CF - BANCO DE BATERIAS",
        ...     "./tmp/DynamicChecklist_SubPM.xlsx"
        ... )
    """
    try:
        # Cargar configuraciones
        config = load_config(env=env)
        postgres_config = config.get("postgress", {})
        dynamic_checklist_config = config.get("dynamic_checklist", {})
        
        if not postgres_config:
            raise ValueError("No se encontró configuración 'postgress' en config YAML")
        if not dynamic_checklist_config:
            raise ValueError("No se encontró configuración 'dynamic_checklist' en config YAML")
        
        filepath_str = str(filepath)
        if not Path(filepath_str).exists():
            raise FileNotFoundError(f"Archivo no encontrado: {filepath_str}")
        
        # Crear instancia del loader base
        loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=dynamic_checklist_config
        )
        
        # Usar fecha_carga proporcionada o generar una nueva
        if fecha_carga is None:
            fecha_carga = datetime.now()
        
        schema = dynamic_checklist_config.get('schema', 'raw')
        load_mode = dynamic_checklist_config.get('load_mode', 'append')
        
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Procesando tabla: {tabla_sql}")
        logger.info(f"   📋 Pestaña Excel: {nombre_pestana}")
        logger.info(f"   🗄️  Tabla destino: {schema}.{tabla_sql}")
        logger.info(f"   📅 Fecha de carga: {fecha_carga}")
        logger.info(f"{'='*80}")
        
        # Cargar mapeo de columnas
        try:
            columnas = traerjson(
                archivo='config/columnas/columns_map.json',
                valor=tabla_sql
            )
            if columnas:
                logger.info(f"✅ Mapeo cargado: {len(columnas)} columnas mapeadas")
            else:
                logger.warning(f"⚠️  No se encontró mapeo de columnas para {tabla_sql}, continuando sin mapeo...")
                columnas = None
        except Exception as e:
            logger.error(f"❌ Error al cargar mapeo de columnas para {tabla_sql}: {str(e)}")
            logger.warning(f"⚠️  Continuando sin mapeo de columnas...")
            columnas = None
        
        # Verificar datos
        logger.info(f"🔍 Verificando estructura de datos para {tabla_sql}...")
        verificacion = loader.verificar_datos(
            data=filepath_str,
            column_mapping=columnas,
            sheet_name=nombre_pestana,
            strictreview=False,
            numerofilasalto=0,
            table_name=tabla_sql
        )
        logger.info(f"✅ Verificación exitosa: {verificacion.get('etl_msg', 'OK')}")
        
        # Cargar datos
        logger.info(f"💾 Cargando datos de '{nombre_pestana}' hacia {schema}.{tabla_sql}...")
        resultado = loader.load_data(
            data=filepath_str,
            sheet_name=nombre_pestana,
            column_mapping=columnas,
            numerofilasalto=0,
            table_name=tabla_sql,
            schema=schema,
            modo=load_mode,
            fecha_carga=fecha_carga
        )
        
        logger.info(f"✅ {tabla_sql} cargada exitosamente")
        logger.info(f"   📊 Resultado: {resultado.get('etl_msg', 'OK')}")
        
        return {
            'status': 'success',
            'code': 200,
            'etl_msg': resultado.get('etl_msg', f"Tabla {tabla_sql} cargada exitosamente"),
            'tabla': tabla_sql,
            'pestana': nombre_pestana,
            'resultado': resultado
        }
        
    except Exception as e:
        error_msg = f"Error al cargar tabla {tabla_sql}: {str(e)}"
        logger.error(f"❌ {error_msg}")
        logger.error(f"   Tipo de error: {type(e).__name__}")
        logger.error(f"   Detalles completos: {str(e)}")
        import traceback
        logger.error(f"   Traceback completo:")
        for line in traceback.format_exc().split('\n'):
            if line.strip():
                logger.error(f"      {line}")
        
        return {
            'status': 'error',
            'code': 500,
            'etl_msg': error_msg,
            'tabla': tabla_sql,
            'pestana': nombre_pestana,
            'error': f"{type(e).__name__}: {str(e)}",
            'tipo_error': type(e).__name__
        }


def load_dynamic_checklist(filepath: Optional[PathLike] = None, env: str = None) -> dict:
    """
    Carga los datos extraídos de Dynamic Checklist hacia PostgreSQL.
    
    Procesa 11 pestañas del Excel y las carga en sus respectivas tablas:
    - Cada pestaña corresponde a una tabla en raw.*
    - Usa el mapeo de columnas desde columns_map.json
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
    try:
        # Cargar configuraciones desde config_dev.yaml o config_prod.yaml
        config = load_config(env=env)
        postgres_config = config.get("postgress", {})
        dynamic_checklist_config = config.get("dynamic_checklist", {})

        # Validar que existen las configuraciones necesarias
        if not postgres_config:
            raise ValueError("No se encontró configuración 'postgress' en config YAML")
        if not dynamic_checklist_config:
            raise ValueError("No se encontró configuración 'dynamic_checklist' en config YAML")

        # Determinar ruta del archivo
        if not filepath:
            local_dir = dynamic_checklist_config.get('local_dir', './tmp')
            filename = dynamic_checklist_config.get('specific_filename', 'DynamicChecklist_SubPM.xlsx')
            filepath = f"{local_dir}/{filename}"

        filepath_str = str(filepath)
        logger.info(f"📁 Cargando datos desde: {filepath_str}")

        # Validar que el archivo existe
        if not Path(filepath_str).exists():
            raise FileNotFoundError(f"Archivo no encontrado: {filepath_str}")

        # Crear instancia del loader base (usará schema y config general)
        loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=dynamic_checklist_config
        )

        # Validar conexión
        logger.info("🔌 Validando conexión a PostgreSQL...")
        loader.validar_conexion()

        # Capturar fecha y hora de inicio del proceso (para columna fechacarga)
        fecha_carga_inicio = datetime.now()
        logger.info(f"📅 Fecha de carga establecida: {fecha_carga_inicio}")

        # Procesar cada tabla/pestaña
        schema = dynamic_checklist_config.get('schema', 'raw')
        # Usar variable específica para dynamic_checklist (siempre append para histórico)
        # if_exists se mantiene para otros módulos que hacen replace
        load_mode = dynamic_checklist_config.get('load_mode', 'append')
        resultados = []
        errores = []

        logger.info(f"🚀 Iniciando carga de {len(TABLAS_DYNAMIC_CHECKLIST)} tablas...")

        for tabla_sql, nombre_pestana in TABLAS_DYNAMIC_CHECKLIST.items():
            try:
                logger.info(f"\n{'='*80}")
                logger.info(f"📊 [TABLA {list(TABLAS_DYNAMIC_CHECKLIST.keys()).index(tabla_sql) + 1}/{len(TABLAS_DYNAMIC_CHECKLIST)}] Procesando: {tabla_sql}")
                logger.info(f"   📋 Pestaña Excel: {nombre_pestana}")
                logger.info(f"   🗄️  Tabla destino: {schema}.{tabla_sql}")
                logger.info(f"{'='*80}")

                # Cargar mapeo de columnas para esta tabla
                logger.info(f"📋 Cargando mapeo de columnas para {tabla_sql}...")
                try:
                    columnas = traerjson(
                        archivo='config/columnas/columns_map.json',
                        valor=tabla_sql
                    )
                    if columnas:
                        logger.info(f"✅ Mapeo cargado: {len(columnas)} columnas mapeadas")
                    else:
                        logger.warning(f"⚠️  No se encontró mapeo de columnas para {tabla_sql}, continuando sin mapeo...")
                        columnas = None
                except Exception as e:
                    logger.error(f"❌ Error al cargar mapeo de columnas para {tabla_sql}: {str(e)}")
                    logger.warning(f"⚠️  Continuando sin mapeo de columnas...")
                    columnas = None

                # Verificar datos
                logger.info(f"🔍 Verificando estructura de datos para {tabla_sql}...")
                try:
                    verificacion = loader.verificar_datos(
                        data=filepath_str,
                        column_mapping=columnas,
                        sheet_name=nombre_pestana,
                        strictreview=False,
                        numerofilasalto=0,
                        table_name=tabla_sql
                    )
                    logger.info(f"✅ Verificación exitosa: {verificacion.get('etl_msg', 'OK')}")
                except Exception as e:
                    logger.error(f"❌ Error en verificación de datos para {tabla_sql}: {str(e)}")
                    logger.error(f"   Detalles: {type(e).__name__}: {str(e)}")
                    raise  # Re-lanzar para que se capture en el bloque de errores

                # Cargar datos
                logger.info(f"💾 Cargando datos de '{nombre_pestana}' hacia {schema}.{tabla_sql}...")
                try:
                    resultado = loader.load_data(
                        data=filepath_str,
                        sheet_name=nombre_pestana,
                        column_mapping=columnas,
                        numerofilasalto=0,
                        table_name=tabla_sql,
                        schema=schema,
                        modo=load_mode,
                        fecha_carga=fecha_carga_inicio
                    )
                    
                    filas_cargadas = resultado.get('etl_msg', '')
                    logger.info(f"✅ {tabla_sql} cargada exitosamente")
                    logger.info(f"   📊 Resultado: {filas_cargadas}")
                    
                    resultados.append({
                        'tabla': tabla_sql,
                        'pestana': nombre_pestana,
                        'resultado': resultado
                    })
                except Exception as e:
                    logger.error(f"❌ Error al cargar datos en {tabla_sql}: {str(e)}")
                    logger.error(f"   Tipo de error: {type(e).__name__}")
                    logger.error(f"   Detalles completos: {str(e)}")
                    raise  # Re-lanzar para que se capture en el bloque de errores

            except FileNotFoundError as e:
                error_msg = f"Archivo o pestaña no encontrada para {tabla_sql}"
                logger.error(f"❌ {error_msg}")
                logger.error(f"   Pestaña buscada: {nombre_pestana}")
                logger.error(f"   Archivo: {filepath_str}")
                logger.error(f"   Error: {str(e)}")
                errores.append({
                    'tabla': tabla_sql,
                    'pestana': nombre_pestana,
                    'error': f"FileNotFoundError: {str(e)}",
                    'tipo_error': 'FileNotFoundError'
                })
                continue
                
            except ValueError as e:
                error_msg = f"Error de validación para {tabla_sql}"
                logger.error(f"❌ {error_msg}")
                logger.error(f"   Detalles: {str(e)}")
                logger.error(f"   Tipo: ValueError")
                errores.append({
                    'tabla': tabla_sql,
                    'pestana': nombre_pestana,
                    'error': f"ValueError: {str(e)}",
                    'tipo_error': 'ValueError'
                })
                continue
                
            except Exception as e:
                error_msg = f"Error inesperado al cargar {tabla_sql}"
                logger.error(f"❌ {error_msg}")
                logger.error(f"   Tabla: {tabla_sql}")
                logger.error(f"   Pestaña: {nombre_pestana}")
                logger.error(f"   Tipo de error: {type(e).__name__}")
                logger.error(f"   Mensaje: {str(e)}")
                import traceback
                logger.error(f"   Traceback completo:")
                for line in traceback.format_exc().split('\n'):
                    if line.strip():
                        logger.error(f"      {line}")
                
                errores.append({
                    'tabla': tabla_sql,
                    'pestana': nombre_pestana,
                    'error': f"{type(e).__name__}: {str(e)}",
                    'tipo_error': type(e).__name__
                })
                # Continuar con las siguientes tablas aunque una falle
                continue

        # Resumen final
        logger.info(f"\n{'='*80}")
        logger.info("📊 RESUMEN FINAL DE CARGA")
        logger.info(f"{'='*80}")
        logger.info(f"📁 Archivo procesado: {filepath_str}")
        logger.info(f"📊 Total de tablas a procesar: {len(TABLAS_DYNAMIC_CHECKLIST)}")
        logger.info(f"✅ Tablas cargadas exitosamente: {len(resultados)}")
        
        if resultados:
            logger.info(f"\n✅ TABLAS CARGADAS EXITOSAMENTE ({len(resultados)}):")
            for res in resultados:
                tabla = res['tabla']
                pestana = res['pestana']
                msg = res['resultado'].get('etl_msg', 'OK')
                logger.info(f"   ✓ {tabla:40s} ({pestana:40s}) → {msg}")
        
        if errores:
            logger.error(f"\n❌ TABLAS CON ERRORES ({len(errores)}):")
            for error in errores:
                tabla = error['tabla']
                pestana = error['pestana']
                tipo = error.get('tipo_error', 'Unknown')
                msg_error = error['error']
                logger.error(f"   ✗ {tabla:40s} ({pestana:40s})")
                logger.error(f"      Tipo: {tipo}")
                logger.error(f"      Error: {msg_error}")
        else:
            logger.info(f"\n🎉 Todas las tablas se cargaron exitosamente!")
        
        logger.info(f"{'='*80}")

        # Retornar resultado consolidado
        if errores:
            return {
                'status': 'partial_success' if resultados else 'error',
                'code': 207 if resultados else 500,
                'etl_msg': f"Carga completada: {len(resultados)} exitosas, {len(errores)} con errores",
                'resultados': resultados,
                'errores': errores
            }
        else:
            return {
                'status': 'success',
                'code': 200,
                'etl_msg': f"Todas las {len(resultados)} tablas cargadas exitosamente",
                'resultados': resultados
            }

    except FileNotFoundError as e:
        msg = f"Archivo no encontrado: {e}"
        logger.error(msg)
        raise
    except ValueError as e:
        msg = f"Error de configuración: {e}"
        logger.error(msg)
        raise
    except Exception as e:
        msg = f"Error durante la carga de datos: {e}"
        logger.error(msg)
        raise
