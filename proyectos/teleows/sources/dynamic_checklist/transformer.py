from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def transformar_dynamic_checklist(archivo_descargado: PathLike) -> str:
    """
    Placeholder para futuras transformaciones del reporte Dynamic Checklist.
    """
    path = Path(archivo_descargado)
    logger.info(
        "Transformación Dynamic Checklist pendiente de implementación. Archivo sin cambios: %s",
        path,
    )
    return str(path)
