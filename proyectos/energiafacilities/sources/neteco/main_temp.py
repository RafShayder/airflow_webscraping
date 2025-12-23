from .stractor import stractor_neteco
from .transformer import transformer_neteco
from .loader import load_neteco
from core.utils import setup_logging
from .run_sp import correr_sp_neteco
path_stractor=stractor_neteco()
#path_stractor="tmp/neteco/HistoricalData_20251219000000_20251219003000_20251219110351.zip"
if(True):
    path_transformer=transformer_neteco(path_stractor)
    if(path_transformer):
        retorno=load_neteco(path_transformer)
        if(retorno):
            correr_sp_neteco() 
setup_logging(level="INFO")


