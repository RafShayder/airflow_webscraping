"""
Tests de integración para BaseLoaderPostgres.
Usa mocks para simular PostgreSQL sin necesidad de una BD real.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, call
from core.base_loader import BaseLoaderPostgres


@pytest.fixture
def loader(postgres_config, loader_config):
    """Instancia de loader para tests."""
    return BaseLoaderPostgres(postgres_config, loader_config)


@pytest.fixture
def mock_connection():
    """Mock de conexión PostgreSQL."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


class TestInsertDataframe:
    """Tests para insert_dataframe()"""

    @patch('core.base_loader.psycopg2.connect')
    @patch('core.base_loader.execute_values')
    def test_truncates_on_replace_mode(self, mock_execute_values, mock_connect, loader):
        """Modo 'replace' debe ejecutar TRUNCATE antes de insertar."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)  # Tabla existe
        mock_cursor.fetchall.return_value = [("col1",), ("col2",)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        loader.insert_dataframe(df, modo="replace")

        # Verificar que TRUNCATE fue llamado
        execute_calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any("TRUNCATE" in str(c) for c in execute_calls)

    @patch('core.base_loader.psycopg2.connect')
    @patch('core.base_loader.execute_values')
    def test_no_truncate_on_append_mode(self, mock_execute_values, mock_connect, loader):
        """Modo 'append' no debe ejecutar TRUNCATE."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)
        mock_cursor.fetchall.return_value = [("col1",)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        df = pd.DataFrame({"col1": [1, 2]})

        loader.insert_dataframe(df, modo="append")

        execute_calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert not any("TRUNCATE" in str(c) for c in execute_calls)

    @patch('core.base_loader.psycopg2.connect')
    def test_fail_mode_raises_if_table_exists(self, mock_connect, loader):
        """Modo 'fail' debe lanzar error si la tabla existe."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)  # Tabla existe
        mock_cursor.fetchall.return_value = [("col1",)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        df = pd.DataFrame({"col1": [1]})

        with pytest.raises(ValueError, match="ya existe"):
            loader.insert_dataframe(df, modo="fail")

    def test_empty_dataframe_raises(self, loader):
        """DataFrame vacío debe lanzar error."""
        df = pd.DataFrame()

        with pytest.raises(ValueError, match="DataFrame vacío"):
            loader.insert_dataframe(df)

    @patch('core.base_loader.psycopg2.connect')
    @patch('core.base_loader.execute_values')
    def test_batched_insert(self, mock_execute_values, mock_connect, loader):
        """Inserción debe hacerse en batches."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)
        mock_cursor.fetchall.return_value = [("col1",)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        # DataFrame con 250 filas, batch de 100
        df = pd.DataFrame({"col1": range(250)})

        loader.insert_dataframe(df, batch_size=100, modo="append")

        # Debe llamar execute_values 3 veces (100 + 100 + 50)
        assert mock_execute_values.call_count == 3


class TestValidarConexion:
    """Tests para validar_conexion()"""

    @patch('core.base_loader.psycopg2.connect')
    def test_successful_connection(self, mock_connect, loader):
        """Conexión exitosa debe retornar success."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = loader.validar_conexion()

        assert result["status"] == "success"
        assert result["code"] == 200
        mock_conn.close.assert_called_once()

    @patch('core.base_loader.psycopg2.connect')
    def test_failed_connection_raises(self, mock_connect, loader):
        """Conexión fallida debe lanzar excepción."""
        mock_connect.side_effect = Exception("Connection refused")

        with pytest.raises(Exception, match="Connection refused"):
            loader.validar_conexion()


class TestLoadData:
    """Tests para load_data()"""

    @patch.object(BaseLoaderPostgres, 'insert_dataframe')
    def test_load_from_dataframe(self, mock_insert, loader):
        """load_data debe aceptar DataFrame directamente."""
        df = pd.DataFrame({"col1": [1, 2, 3]})
        mock_insert.return_value = {"status": "success"}

        result = loader.load_data(df)

        assert mock_insert.called

    @patch.object(BaseLoaderPostgres, 'insert_dataframe')
    def test_load_with_column_mapping(self, mock_insert, loader):
        """load_data debe aplicar mapeo de columnas."""
        df = pd.DataFrame({"Excel Col": [1, 2]})
        mapping = {"db_col": "Excel Col"}
        mock_insert.return_value = {"status": "success"}

        loader.load_data(df, column_mapping=mapping)

        # Verificar que el DataFrame pasado a insert tiene las columnas mapeadas
        call_args = mock_insert.call_args
        df_passed = call_args[0][0]
        assert "db_col" in df_passed.columns


class TestGetTableColumns:
    """Tests para _get_table_columns()"""

    @patch('core.base_loader.psycopg2.connect')
    def test_returns_column_set(self, mock_connect, loader):
        """Debe retornar set de nombres de columnas."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("id",), ("nombre",), ("fecha",)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        result = loader._get_table_columns("test_table")

        assert result == {"id", "nombre", "fecha"}

    @patch('core.base_loader.psycopg2.connect')
    def test_empty_table_returns_empty_set(self, mock_connect, loader):
        """Tabla sin columnas debe retornar set vacío."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        result = loader._get_table_columns("empty_table")

        assert result == set()
