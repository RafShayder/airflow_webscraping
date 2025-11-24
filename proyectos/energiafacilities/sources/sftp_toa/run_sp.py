import logging
from core.base_run_sp import run_sp

logger = logging.getLogger(__name__)

def correr_sp_toa():
    """Ejecuta el stored procedure de carga para sftp_toa"""
    run_sp("sftp_toa")


