"""
Prueba de auto-descubrimiento de Connections y Variables.

Este módulo contiene las funciones de prueba para demostrar cómo funciona
el auto-descubrimiento cuando una sección NO está registrada en section_mapping
de utils.py.
"""

import logging
from energiafacilities.core.utils import load_config

logger = logging.getLogger(__name__)


def test_autodescubrimiento_connection(**kwargs):
    """
    Prueba el auto-descubrimiento de Connection para una sección nueva.
    
    La sección "mi_api_test" NO está en section_mapping, por lo que el sistema
    automáticamente buscará la Connection "mi_api_test_dev" (o "mi_api_test").
    
    Returns:
        dict: Configuración cargada desde la Connection, o dict vacío si no existe
    
    Ejemplo de Connection a crear en Airflow:
        Connection Id: mi_api_test_dev
        Connection Type: Generic
        Host: 10.0.0.1
        Port: 8080
        Login: usuario_test
        Password: password_test
        Extra (JSON): {"api_key": "abc123", "endpoint": "/api/v1"}
    """
    logger.info("Probando auto-descubrimiento de Connection")
    logger.info("Buscando Connection: 'mi_api_test_dev' o 'mi_api_test'")
    
    config = load_config(env='dev')
    mi_api_config = config.get("mi_api_test", {})
    
    if mi_api_config:
        logger.info("✅ Connection encontrada usando auto-descubrimiento!")
        logger.info(f"Campos encontrados: {list(mi_api_config.keys())}")
        logger.info("")
        
        for key, value in mi_api_config.items():
            if "pass" in key.lower() or "password" in key.lower():
                logger.info(f"  {key}: ***")
            else:
                logger.info(f"  {key}: {value}")
        
        logger.info("")
        logger.info("✅ Auto-descubrimiento funcionando correctamente!")
    else:
        logger.warning("⚠️  No se encontró Connection 'mi_api_test_dev'")
        logger.info("")
        logger.info("Para probar el auto-descubrimiento, crea en Airflow una Connection:")
        logger.info("  - Conn Id: mi_api_test_dev")
        logger.info("  - Conn Type: Generic")
        logger.info("  - Host: 10.0.0.1")
        logger.info("  - Port: 8080")
        logger.info("  - Login: usuario_test")
        logger.info("  - Password: password_test")
        logger.info("  - Extra (JSON): {\"api_key\": \"abc123\", \"endpoint\": \"/api/v1\"}")
        logger.info("")
        logger.info("Nota: El auto-descubrimiento busca:")
        logger.info("  1. Connection específica: 'mi_api_test_dev'")
        logger.info("  2. Connection genérica: 'mi_api_test'")
    
    return mi_api_config

