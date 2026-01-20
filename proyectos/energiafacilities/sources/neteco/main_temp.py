import logging
from core.utils import setup_logging
setup_logging()
logger = logging.getLogger("sources.neteco.main_temp")
logger.info("Iniciando flujo NetEco (simulado)")

from .scraper import scraper_neteco
from .transformer import transformer_neteco
from .loader import load_neteco
from .run_sp import correr_sp_neteco
path_stractor=scraper_neteco()
#path_stractor="tmp/neteco/HistoricalData_20251219000000_20251219003000_20251219110351.zip"
if(True):
    path_transformer=transformer_neteco(path_stractor)
    if(path_transformer):
        retorno=load_neteco(path_transformer)
        if(retorno):
            correr_sp_neteco() 

