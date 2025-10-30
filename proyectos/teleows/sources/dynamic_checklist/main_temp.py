from __future__ import annotations

import logging

from teleows.core.utils import setup_logging
from teleows.sources.dynamic_checklist.stractor import extraer_dynamic_checklist
from teleows.sources.dynamic_checklist.transformer import transformar_dynamic_checklist
from teleows.sources.dynamic_checklist.loader import load_dynamic_checklist
from teleows.sources.dynamic_checklist.run_sp import correr_sp_dynamic_checklist


def main() -> None:
    setup_logging("INFO")
    logger = logging.getLogger(__name__)

    logger.info("=== Pipeline Dynamic Checklist (modo local) ===")
    archivo_descargado = extraer_dynamic_checklist()
    archivo_transformado = transformar_dynamic_checklist(archivo_descargado)
    load_dynamic_checklist(archivo_transformado)
    correr_sp_dynamic_checklist()
    logger.info("Pipeline Dynamic Checklist completado.")


if __name__ == "__main__":
    main()
