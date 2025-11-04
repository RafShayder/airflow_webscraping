# Todo, okey, solo falta confiurar y cambiar el nombre de archivo
from sources.clientes_libres.stractor import extraersftp_clienteslibres
from sources.clientes_libres.transformer import transformer_clienteslibres
from sources.clientes_libres.loader import load_clienteslibres
from sources.clientes_libres.run_sp import correr_sp_clienteslibres
from core.utils import setup_logging


setup_logging("INFO")

resultado_extract=extraersftp_clienteslibres()
pathextraida=resultado_extract.get("ruta") if isinstance(resultado_extract, dict) else resultado_extract
pathtransformacion=transformer_clienteslibres(pathextraida)
load_clienteslibres(pathtransformacion)
correr_sp_clienteslibres()

