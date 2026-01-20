import logging
from core.base_run_sp import run_sp

logger = logging.getLogger(__name__)

def correr_sftp_energia_DA(): #sftp_energia
    run_sp("sftp_energia",sp_name='sp_carga_DA')

def correr_sftp_energia_PD(): #sftp_energia
    run_sp("sftp_energia",sp_name='sp_carga_PD')
   