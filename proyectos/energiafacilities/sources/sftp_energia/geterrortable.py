from core.geterrortable import get_save_errors

import logging
logger = logging.getLogger(__name__)



def get_save_errors_PD():
    get_save_errors(table_name= "table_PD",configyaml= "sftp_energia",filename= "data_errors_PD.xlsx")
    
def get_save_errors_DA():
    get_save_errors(table_name= "table_DA",configyaml= "sftp_energia",filename= "data_errors_PD.xlsx")