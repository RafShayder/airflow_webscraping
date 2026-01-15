"""
Fixtures comunes para tests de energiafacilities.
"""
import pytest
import pandas as pd
import sys
from pathlib import Path

# Agregar el path del proyecto para imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def sample_dataframe():
    """DataFrame de ejemplo para tests."""
    return pd.DataFrame({
        "Columna A": [1, 2, 3],
        "Columna B": ["a", "b", "c"],
        "Columna C": [1.1, 2.2, 3.3]
    })


@pytest.fixture
def postgres_config():
    """Configuración de PostgreSQL para tests (mock)."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass"
    }


@pytest.fixture
def loader_config():
    """Configuración de carga para tests."""
    return {
        "schema": "raw",
        "table": "test_table",
        "if_exists": "replace",
        "chunksize": 1000
    }


@pytest.fixture
def sftp_connect_config():
    """Configuración de conexión SFTP para tests (mock)."""
    return {
        "host": "sftp.example.com",
        "port": 22,
        "username": "test_user",
        "password": "test_pass"
    }


@pytest.fixture
def sftp_paths_config():
    """Configuración de rutas SFTP para tests."""
    return {
        "remote_dir": "/remote/input",
        "local_dir": "/tmp/test_output",
        "processed_dir": "/remote/processed",
        "error_dir": "/remote/error"
    }
