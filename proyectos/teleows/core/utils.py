from __future__ import annotations

import logging
import sys
from typing import Any, Dict, Optional

from teleows.config import TeleowsSettings

logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO") -> None:
    """
    Configura un logger simple hacia stdout, emulando el comportamiento usado
    en EnergiaFacilities para mantener un estilo homogéneo entre proyectos.
    """
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=[handler],
    )


def load_settings(overrides: Optional[Dict[str, Any]] = None) -> TeleowsSettings:
    """
    Genera un TeleowsSettings aplicando overrides opcionales.

    Este helper permite que los módulos de ``sources`` utilicen un punto único
    de acceso a la configuración, tal como ocurre con ``load_config`` en
    EnergiaFacilities.
    """
    overrides = overrides or {}
    settings = TeleowsSettings.load_with_overrides(overrides)
    logger.debug("TeleowsSettings generado con overrides: %s", sorted(overrides.keys()))
    return settings
