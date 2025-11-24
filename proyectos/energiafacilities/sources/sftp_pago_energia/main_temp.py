from core.utils import  setup_logging
from sources.sftp_pago_energia.run_sp import correr_sftp_pago_energia
from sources.sftp_pago_energia.stractor import extraersftp_pago_energia
from sources.sftp_pago_energia.loader import load_sftp_base_sitos
from sources.sftp_pago_energia.geterrortable import get_save_errors_energia

setup_logging(level="DEBUG")
archivo=extraersftp_pago_energia()
if archivo:
    load_sftp_base_sitos(archivo)
    correr_sftp_pago_energia()
    get_save_errors_energia()