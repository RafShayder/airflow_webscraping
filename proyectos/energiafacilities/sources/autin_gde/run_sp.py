import logging
from core.base_run_sp import run_sp

logger = logging.getLogger(__name__)


def correr_sp_gde():
    """Ejecuta el SP ods.sp_cargar_web_hm_autin_infogeneral para GDE."""
    run_sp("", sp_value="ods.sp_cargar_web_hm_autin_infogeneral")
