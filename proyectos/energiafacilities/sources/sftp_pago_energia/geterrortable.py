from core.geterrortable import get_save_errors

import logging
logger = logging.getLogger(__name__)



def get_save_errors_energia():
    get_save_errors(configyaml= "sftp_pago_energia",filename= "data_errors_energia_pago.xlsx")
    
