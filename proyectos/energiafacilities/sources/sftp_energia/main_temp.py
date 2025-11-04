from sources.sftp_energia.stractor import extraersftp_energia_PD,extraersftp_energia_DA
from core.utils import setup_logging
from sources.sftp_energia.loader import load_sftp_energia_DA, load_sftp_energia_PD
from sources.sftp_energia.run_sp import correr_sftp_energia_PD,correr_sftp_energia_DA
from sources.sftp_energia.geterrortable import get_save_errors_PD, get_save_errors_DA

setup_logging(level="INFO")


sftp_energia_PD=extraersftp_energia_PD("202509")
load_sftp_energia_PD(filepath=sftp_energia_PD)
correr_sftp_energia_PD()
get_save_errors_PD()

sftp_energia_DA=extraersftp_energia_DA("202509")
load_sftp_energia_DA(filepath=sftp_energia_DA)
correr_sftp_energia_DA()
get_save_errors_DA()
get_save_errors_DA()

"""
    extraersftp_energia_PD >> load_sftp_energia
    load_sftp_energia >> correr_sftp_energia
    extraersftp_energia_DA >> load_sftp_energia
    load_sftp_energia >> correr_sftp_energia
    
"""