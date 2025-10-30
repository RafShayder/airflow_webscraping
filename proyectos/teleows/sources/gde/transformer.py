from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def transformar_gde(archivo_descargado: PathLike) -> str:
    """
    Placeholder de transformaciones para el pipeline GDE.

    Actualmente no se requiere modificación adicional, por lo que se retorna la
    ruta original. Esta función deja listo el punto de inserción para futuras
    reglas de limpieza/enriquecimiento.
    """
    path = Path(archivo_descargado)
    logger.info("Transformación GDE pendiente de implementación. Archivo sin cambios: %s", path)
    return str(path)
