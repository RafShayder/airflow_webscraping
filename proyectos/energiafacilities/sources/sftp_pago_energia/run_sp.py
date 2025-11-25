import logging
from core.base_run_sp import run_sp

logger = logging.getLogger(__name__)

def correr_sftp_pago_energia(): #sftp_energia
    run_sp("sftp_pago_energia")