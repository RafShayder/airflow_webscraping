from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def load_dynamic_checklist(archivo_transformado: PathLike) -> str:
    """
    Inserta el resultado del scraper Dynamic Checklist en el destino final.

    Todavía no se ha definido el proceso de ingestión, por lo que la función
    mantiene la firma y devuelve la ruta original. Cuando se establezca la
    carga hacia Postgres u otro sistema, el código se añadirá aquí.
    """
    path = Path(archivo_transformado)
    logger.info(
        "Carga Dynamic Checklist pendiente de implementación. Archivo disponible en: %s",
        path,
    )
    return str(path)
