from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def correr_sp_gde(identificador: str | None = None) -> None:
    """
    Punto de entrada para ejecutar stored procedures asociados al pipeline GDE.

    ``identificador`` permite seleccionar SPs específicos cuando existan.
    Por ahora solo se deja trazabilidad para que, al implementar la lógica de
    ingestión, se mantenga una firma consistente con los otros proyectos.
    """
    logger.info(
        "Ejecución de stored procedure GDE pendiente de implementación%s.",
        f" (identificador={identificador})" if identificador else "",
    )
