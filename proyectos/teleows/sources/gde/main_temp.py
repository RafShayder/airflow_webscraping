from __future__ import annotations

import logging

from teleows.core.utils import setup_logging
from teleows.sources.gde.stractor import extraer_gde
from teleows.sources.gde.loader import load_gde
from teleows.sources.gde.run_sp import correr_sp_gde


def main() -> None:
    """
    Pipeline ETL completo para GDE (Console GDE Export).

    Flujo:
        1. Extract: Web scraping → Descarga Excel
        2. Load: Excel → PostgreSQL (RAW layer)
        3. Transform: Stored Procedure (RAW → ODS)

    Notas:
        - No hay transformer.py (transformación se hace en BD con SP)
        - Sigue el patrón de energiafacilities/webindra
        - Logs completos en cada paso
    """
    setup_logging("INFO")
    logger = logging.getLogger(__name__)

    logger.info("=== Pipeline ETL GDE ===")

    # 1. EXTRACT: Web scraping → descarga Excel
    logger.info("PASO 1/3: Extrayendo datos (Web Scraping)...")
    archivo_descargado = extraer_gde()
    logger.info(f"Archivo descargado: {archivo_descargado}")

    # 2. LOAD: Excel → PostgreSQL (RAW)
    logger.info("PASO 2/3: Cargando datos a PostgreSQL (RAW)...")
    resultado_load = load_gde(archivo_descargado)
    logger.info(f"Carga completada: {resultado_load['etl_msg']}")

    # 3. TRANSFORM: SP ejecuta RAW → ODS
    logger.info("PASO 3/3: Ejecutando transformación (Stored Procedure)...")
    resultado_sp = correr_sp_gde()
    logger.info(f"Transformación completada: {resultado_sp['etl_msg']}")

    logger.info("=== Pipeline ETL GDE completado exitosamente ===")


if __name__ == "__main__":
    main()
