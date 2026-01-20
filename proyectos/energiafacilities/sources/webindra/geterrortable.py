from core.geterrortable import get_save_errors

import logging
logger = logging.getLogger(__name__)



def get_save_errors_indra(): #
    get_save_errors(table_name= "table",configyaml= "webindra_energia",filename= "data_errors_indra.xlsx")
    
    

