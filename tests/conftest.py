"""
Configuración común para tests
Fixtures y mocks compartidos entre tests
"""
import pytest
import sys
import os
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import tempfile
import pandas as pd

# Agregar el directorio del proyecto al path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "proyectos" / "energiafacilities"))


@pytest.fixture
def temp_dir():
    """Crea un directorio temporal para tests"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_dataframe():
    """DataFrame de ejemplo para tests"""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'nombre': ['Juan', 'María', 'Pedro'],
        'edad': [25, 30, 35]
    })


@pytest.fixture
def sample_csv_file(temp_dir, sample_dataframe):
    """Crea un archivo CSV de ejemplo"""
    csv_path = os.path.join(temp_dir, 'test_data.csv')
    sample_dataframe.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def sample_excel_file(temp_dir, sample_dataframe):
    """Crea un archivo Excel de ejemplo"""
    excel_path = os.path.join(temp_dir, 'test_data.xlsx')
    sample_dataframe.to_excel(excel_path, index=False)
    return excel_path


@pytest.fixture
def mock_sftp_client():
    """Mock de paramiko.SFTPClient"""
    mock = MagicMock()
    mock.listdir.return_value = ['file1.txt', 'file2.xlsx']
    mock.listdir_attr.return_value = []
    mock.get.return_value = None
    mock.rename.return_value = None
    mock.stat.side_effect = FileNotFoundError("File not found")  # Por defecto, archivo no existe
    mock.close.return_value = None
    return mock


@pytest.fixture
def mock_sftp_transport():
    """Mock de paramiko.Transport"""
    mock = MagicMock()
    mock.connect.return_value = None
    mock.close.return_value = None
    return mock


@pytest.fixture
def mock_postgres_connection():
    """Mock de conexión PostgreSQL"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [('id',), ('nombre',), ('edad',)]
    mock_cursor.fetchall.return_value = [(1, 'Juan', 25), (2, 'María', 30)]
    mock_cursor.fetchone.return_value = (True,)

    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=False)

    return mock_conn


@pytest.fixture
def mock_postgres_engine():
    """Mock de SQLAlchemy Engine"""
    mock = MagicMock()
    mock.connect.return_value.__enter__ = Mock()
    mock.connect.return_value.__exit__ = Mock()
    mock.dispose.return_value = None
    return mock


@pytest.fixture
def postgres_config():
    """Configuración de PostgreSQL para tests"""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }


@pytest.fixture
def sftp_config_connect():
    """Configuración de conexión SFTP para tests"""
    return {
        'host': 'sftp.example.com',
        'port': 22,
        'username': 'test_user',
        'password': 'test_pass'
    }


@pytest.fixture
def sftp_config_paths():
    """Configuración de rutas SFTP para tests"""
    return {
        'remote_dir': '/remote/path',
        'local_dir': '/tmp/local/path',
        'specific_filename': 'test_file.xlsx'
    }


@pytest.fixture
def loader_config():
    """Configuración para BaseLoaderPostgres"""
    return {
        'schema': 'public',
        'table': 'test_table',
        'if_exists': 'replace',
        'chunksize': 1000
    }
