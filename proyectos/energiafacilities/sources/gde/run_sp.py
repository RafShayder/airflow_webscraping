from __future__ import annotations

import logging

from energiafacilities.core import load_config
from energiafacilities.core.base_postgress import PostgresConnector

logger = logging.getLogger(__name__)


def correr_sp_gde(config_section: str = "gde", postgres_section: str = "postgress", env: str = None) -> dict:
    """
    Ejecuta el stored procedure para transformar datos de GDE (RAW → ODS).

    Este SP se ejecuta después de cargar los datos a la capa RAW y realiza:
    - Conversión de tipos de datos (VARCHAR → tipos específicos)
    - Limpieza y validación de datos
    - Transformaciones de negocio
    - Carga a la capa ODS (Operational Data Store)

    Args:
        config_section: Nombre de la sección en config YAML con la config de GDE (default: 'gde')
        postgres_section: Nombre de la sección con credenciales de PostgreSQL (default: 'postgress')
        env: Entorno (dev, prod). Si no se proporciona, usa ENV_MODE o 'dev'

    Returns:
        Diccionario con información de la ejecución del SP

    Example:
        >>> from energiafacilities.sources.gde.run_sp import correr_sp_gde
        >>> resultado = correr_sp_gde()
        >>> print(resultado['etl_msg'])

    Raises:
        ValueError: Si no se encuentra la configuración necesaria
        Exception: Si falla la ejecución del SP
    """
    try:
        # Cargar configuraciones desde config_dev.yaml o config_prod.yaml
        config = load_config(env=env)
        postgres_config = config.get(postgres_section, {})
        gde_config = config.get(config_section, {})

        # Validar configuraciones
        if not postgres_config:
            raise ValueError(f"No se encontró configuración '{postgres_section}' en config YAML")
        if not gde_config:
            raise ValueError(f"No se encontró configuración '{config_section}' en config YAML")

        # Obtener nombre del SP
        sp_name = gde_config.get('sp_carga')
        if not sp_name:
            raise ValueError(f"No se encontró 'sp_carga' en la configuración '{config_section}'")

        # Crear conexión a PostgreSQL
        logger.info("Conectando a PostgreSQL para ejecutar stored procedure...")
        postgres = PostgresConnector(postgres_config)
        postgres.validar_conexion()

        # Ejecutar SP
        logger.info(f"Ejecutando stored procedure: {sp_name}")
        postgres.ejecutar(sp_name, tipo='sp')

        # Obtener logs del SP (si existe función de logs)
        try:
            logger.info("Obteniendo logs de ejecución del SP...")
            log_data = postgres.ejecutar(
                "public.log_sp_ultimo_fn",
                parametros=(f'{sp_name}()',),
                tipo='fn'
            )

            if not log_data.empty:
                estado = log_data['estado'].values[0] if 'estado' in log_data.columns else 'unknown'
                mensaje = log_data['msj_error'].values[0] if 'msj_error' in log_data.columns else 'Sin mensaje'
                logger.info(f"Estado SP: {estado}, Detalle: {mensaje}")

                return {
                    "status": "success" if estado == "success" else "warning",
                    "code": 200,
                    "etl_msg": f"SP ejecutado: {estado} - {mensaje}",
                    "sp_name": sp_name,
                    "log_data": log_data.to_dict('records')
                }
        except Exception as log_error:
            # Si no existe la función de logs, continuar sin error
            logger.warning(f"No se pudo obtener log del SP (función de logs no disponible): {log_error}")

        # Cerrar conexión
        postgres.close()

        return {
            "status": "success",
            "code": 200,
            "etl_msg": f"Stored procedure '{sp_name}' ejecutado correctamente",
            "sp_name": sp_name
        }

    except ValueError as e:
        msg = f"Error de configuración: {e}"
        logger.error(msg)
        raise
    except Exception as e:
        msg = f"Error durante la ejecución del SP: {e}"
        logger.error(msg)
        raise
