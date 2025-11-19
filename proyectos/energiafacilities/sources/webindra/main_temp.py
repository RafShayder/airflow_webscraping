 # ok, sin correcciones
from energiafacilities.core.utils import setup_logging
from sources.webindra.stractor import stractor_indra
from sources.webindra.loader import load_indra
from sources.webindra.run_sp import correr_sp_webindra
from sources.webindra.geterrortable import get_save_errors_indra
setup_logging("INFO")
path=stractor_indra()
load_indra(path)
correr_sp_webindra()
get_save_errors_indra()