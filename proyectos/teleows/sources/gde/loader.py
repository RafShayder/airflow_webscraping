from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


def load_gde(archivo_transformado: PathLike) -> str:
    """
    Inserta los datos de GDE en el destino final.

    Aún no existe un proceso de ingestión definido, por lo que este helper solo
    actúa como placeholder y devuelve la ruta recibida. Cuando se defina la
    carga hacia Postgres u otro sistema, la lógica se implementará aquí sin
    impactar al resto del pipeline.
    """
    path = Path(archivo_transformado)
    logger.info("Carga GDE pendiente de implementación. Archivo disponible en: %s", path)
    return str(path)
