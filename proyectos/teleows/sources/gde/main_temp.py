from __future__ import annotations

import logging

from teleows.core.utils import setup_logging
from teleows.sources.gde.stractor import extraer_gde
from teleows.sources.gde.transformer import transformar_gde
from teleows.sources.gde.loader import load_gde
from teleows.sources.gde.run_sp import correr_sp_gde


def main() -> None:
    setup_logging("INFO")
    logger = logging.getLogger(__name__)

    logger.info("=== Pipeline GDE (modo local) ===")
    archivo_descargado = extraer_gde()
    archivo_transformado = transformar_gde(archivo_descargado)
    load_gde(archivo_transformado)
    correr_sp_gde()
    logger.info("Pipeline GDE completado.")


if __name__ == "__main__":
    main()
