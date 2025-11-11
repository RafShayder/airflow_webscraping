import logging
from core.base_run_sp import run_sp

logger = logging.getLogger(__name__)

def correr_sp_basesitios(): #sftp_base_sitios
    run_sp("sftp_base_sitios")

def correr_sp_bitacora(): #sftp_base_sitios
    run_sp("sftp_base_sitios_bitacora")

   