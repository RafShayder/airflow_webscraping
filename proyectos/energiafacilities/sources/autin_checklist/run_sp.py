import logging
from core.base_run_sp import run_sp

logger = logging.getLogger(__name__)


def correr_sp_checklist():
    """Ejecuta el SP ods.sp_validacion_hm_checklist para Dynamic Checklist."""
    run_sp("", sp_value="ods.sp_validacion_hm_checklist")
