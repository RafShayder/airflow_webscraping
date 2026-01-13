import sys
from datetime import datetime, timedelta
from pathlib import Path

# Configurar path para imports cuando se ejecuta directamente
current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[2]))  # /.../energiafacilities

from core.base_stractor import BaseExtractorSFTP
from core.helpers import generar_archivo_especifico
from core.utils import load_config, setup_logging
import logging

logger = logging.getLogger(__name__)


def extraer_base_suministros_activos():
    """
    Extrae archivos de base suministros activos desde SFTP.
    
    Busca archivos que contengan 'base_suministros_activos' en el nombre
    y selecciona el más reciente por fecha de modificación.
    
    Returns:
        str: Ruta del archivo extraído localmente
    """
    config = load_config()
    sftp_config_connect = config.get("sftp_energia_c", {})  # Usa sftp_energia_{env} según entorno
    sftp_config_others = config.get("sftp_base_suministros_activos", {})
    
    Extractor = BaseExtractorSFTP(
        config_connect=sftp_config_connect,
        config_paths=sftp_config_others
    )
    Extractor.validar_conexion()
    Extractor.validate()  # validar datos del sftp
    
    # Buscar archivos que contengan "base_suministros_activos" en el nombre
    archivos_atributos = Extractor.listar_archivos_atributos()
    basearchivo = sftp_config_others.get("specific_filename", "base_suministros_activos")
    
    # Filtrar por el periodo del mes anterior (MMYY) y elegir el más reciente
    hoy = datetime.today()
    ultimo_dia_mes_anterior = hoy.replace(day=1) - timedelta(days=1)
    periodo_ref = f"{ultimo_dia_mes_anterior.month:02d}{ultimo_dia_mes_anterior.year % 100:02d}"

    archivo_mas_reciente = generar_archivo_especifico(
        lista_archivos=archivos_atributos,
        basearchivo=basearchivo,
        periodo=periodo_ref,
        tipo="xlsx",
    )
    if not archivo_mas_reciente:
        raise FileNotFoundError(
            f"No se encontraron archivos que contengan '{basearchivo}' en el directorio SFTP"
        )
    nombrearchivoextraer = archivo_mas_reciente["nombre"]
    
    logger.info(f"Archivo seleccionado: {nombrearchivoextraer} (modificado: {archivo_mas_reciente['fecha_modificacion']})")
    
    metastraccion = Extractor.extract(specific_file=nombrearchivoextraer)
    return metastraccion['ruta']


# Ejecución local (desarrollo/testing)
# Para producción, usar los DAGs de Airflow
# El entorno se determina automáticamente desde ENV_MODE o usa "dev" por defecto
if __name__ == "__main__":
    setup_logging(level="INFO")
    try:
        ruta = extraer_base_suministros_activos()
        logger.info(f"✅ Extracción exitosa. Archivo guardado en: {ruta}")
    except Exception as e:
        logger.error(f"❌ Error en la extracción: {e}", exc_info=True)
        raise
