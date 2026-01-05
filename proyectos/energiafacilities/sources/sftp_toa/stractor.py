import sys
from pathlib import Path

# Configurar path para imports cuando se ejecuta directamente
current_path = Path(__file__).resolve()
sys.path.insert(0, str(current_path.parents[2]))  # /.../energiafacilities

from core.base_stractor import BaseExtractorSFTP
from core.utils import load_config, setup_logging
import logging

logger = logging.getLogger(__name__)


def extraer_toa():
    config = load_config()
    sftp_config_connect = config.get("sftp_daas_c", {})
    sftp_config_others = config.get("sftp_toa", {})
    Extractor = BaseExtractorSFTP(
        config_connect=sftp_config_connect,
        config_paths=sftp_config_others
    )
    Extractor.validar_conexion()
    Extractor.validate()  # validar datos del sftp
    
    # Buscar archivos que contengan "TOA" o "Reporte TOA" en el nombre
    archivos_atributos = Extractor.listar_archivos_atributos()
    basearchivo = sftp_config_others.get("specific_filename", "TOA")
    
    # Filtrar archivos que contengan el texto base (case insensitive)
    archivos_filtrados = [
        f for f in archivos_atributos
        if basearchivo.upper() in f["nombre"].upper() and f["nombre"].lower().endswith(".xlsx")
    ]
    
    if not archivos_filtrados:
        raise FileNotFoundError(
            f"No se encontraron archivos que contengan '{basearchivo}' en el directorio SFTP"
        )
    
    # Seleccionar el archivo más reciente por fecha de modificación
    archivo_mas_reciente = max(archivos_filtrados, key=lambda x: x["fecha_modificacion"])
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
        ruta = extraer_toa()
        logger.info(f"✅ Extracción exitosa. Archivo guardado en: {ruta}")
        print(f"\n✅ Extracción exitosa. Archivo guardado en: {ruta}")
    except Exception as e:
        logger.error(f"❌ Error en la extracción: {e}", exc_info=True)
        print(f"\n❌ Error en la extracción: {e}")
        raise

