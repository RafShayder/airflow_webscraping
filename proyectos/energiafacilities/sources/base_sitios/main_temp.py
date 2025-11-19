# Todo okey,
from sources.base_sitios.stractor import extraer_basedesitios
from core.utils import setup_logging
from sources.base_sitios.loader import loader_basesitios,loader_bitacora_basesitios
from sources.base_sitios.run_sp import correr_sp_basesitios ,correr_sp_bitacora

setup_logging(level="INFO")

#ETL Main

"""
      'owner': 'SigmaAnalytics',
    'start_date': datetime(2025, 10, 6),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    Dag:
        frecuencia: 1 de cada mes: 0:00 del dia 1
"""

rutaextraida=extraer_basedesitios()
loader_basesitios(rutaextraida)
loader_bitacora_basesitios(rutaextraida)
correr_sp_basesitios()
correr_sp_bitacora()
"""
    extraer_basedesitios >>loader_basesitios
    extraer_basedesitios >>loader_bitacora_basesitios
    loader_basesitios >> correr_sp_basesitios
    loader_bitacora_basesitios >> correr_sp_bitacora
"""
