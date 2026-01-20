"""
Tests para BaseExtractorSFTP.
Verifica validación de configuración y procesamiento de DataFrames.
"""
import pytest
import pandas as pd
from core.base_stractor import BaseExtractorSFTP


@pytest.fixture
def extractor(sftp_connect_config, sftp_paths_config):
    """Instancia de extractor SFTP para tests."""
    return BaseExtractorSFTP(sftp_connect_config, sftp_paths_config)


class TestBaseExtractorInit:
    """Tests para inicialización de BaseExtractorSFTP"""

    def test_valid_config(self, sftp_connect_config, sftp_paths_config):
        """Inicialización con configuración válida."""
        extractor = BaseExtractorSFTP(sftp_connect_config, sftp_paths_config)

        assert extractor.conn.host == "sftp.example.com"
        assert extractor.conn.port == 22
        assert extractor.paths.remote_dir == "/remote/input"

    def test_invalid_config_type(self):
        """Debe rechazar configuración que no sea dict."""
        with pytest.raises(ValueError, match="deben ser diccionarios"):
            BaseExtractorSFTP("no es dict", {})

        with pytest.raises(ValueError, match="deben ser diccionarios"):
            BaseExtractorSFTP({}, "no es dict")

    def test_rejects_none_config(self):
        """Debe rechazar None como configuración."""
        with pytest.raises(ValueError):
            BaseExtractorSFTP(None, {})


class TestValidateConfig:
    """Tests para validate()"""

    def test_valid_minimal_config(self):
        """Validación con configuración mínima requerida."""
        connect = {"host": "sftp.test.com", "port": 22, "username": "user"}
        paths = {"remote_dir": "/input", "local_dir": "/output"}

        extractor = BaseExtractorSFTP(connect, paths)
        result = extractor.validate()

        assert result["status"] == "success"
        assert result["code"] == 200

    def test_missing_host(self):
        """Debe fallar si falta host."""
        connect = {"port": 22, "username": "user"}
        paths = {"remote_dir": "/input", "local_dir": "/output"}

        extractor = BaseExtractorSFTP(connect, paths)

        with pytest.raises(ValueError, match="Faltan campos"):
            extractor.validate()

    def test_missing_remote_dir(self):
        """Debe fallar si falta remote_dir."""
        connect = {"host": "sftp.test.com", "port": 22, "username": "user"}
        paths = {"local_dir": "/output"}

        extractor = BaseExtractorSFTP(connect, paths)

        with pytest.raises(ValueError, match="Faltan campos"):
            extractor.validate()

    def test_empty_host(self):
        """Debe fallar si host está vacío."""
        connect = {"host": "", "port": 22, "username": "user"}
        paths = {"remote_dir": "/input", "local_dir": "/output"}

        extractor = BaseExtractorSFTP(connect, paths)

        with pytest.raises(ValueError, match="Faltan campos"):
            extractor.validate()


class TestValidateColumnsDataframe:
    """Tests para validate_columns_dataframe()"""

    def test_valid_columns(self, extractor):
        """Validación con todas las columnas presentes."""
        df = pd.DataFrame({
            "Col Original": [1, 2, 3],
            "Otra Col": ["a", "b", "c"]
        })
        mapping = {
            "col_db": "Col Original",
            "otra_db": "Otra Col"
        }

        result = extractor.validate_columns_dataframe(df, mapping)

        assert result is not None
        assert list(result.columns) == ["col_db", "otra_db"]
        assert len(result) == 3

    def test_missing_columns_returns_none(self, extractor):
        """Debe retornar None si faltan columnas."""
        df = pd.DataFrame({"Col A": [1, 2]})
        mapping = {"col_db": "Col Inexistente"}

        result = extractor.validate_columns_dataframe(df, mapping)

        assert result is None

    def test_partial_columns_returns_none(self, extractor):
        """Debe retornar None si falta al menos una columna del mapping."""
        df = pd.DataFrame({
            "Col A": [1],
            "Col B": [2]
        })
        mapping = {
            "col_a": "Col A",
            "col_c": "Col C"  # No existe
        }

        result = extractor.validate_columns_dataframe(df, mapping)

        assert result is None

    def test_renames_columns_correctly(self, extractor):
        """Verifica que renombre las columnas según el mapping."""
        df = pd.DataFrame({
            "Nombre Completo": ["Ana García"],
            "Fecha Nacimiento": ["1990-01-15"]
        })
        mapping = {
            "nombre": "Nombre Completo",
            "fecha_nac": "Fecha Nacimiento"
        }

        result = extractor.validate_columns_dataframe(df, mapping)

        assert "nombre" in result.columns
        assert "fecha_nac" in result.columns
        assert "Nombre Completo" not in result.columns

    def test_filters_unmapped_columns(self, extractor):
        """Debe filtrar columnas que no están en el mapping."""
        df = pd.DataFrame({
            "Col A": [1],
            "Col B": [2],
            "Col C": [3],
            "Col D": [4]
        })
        mapping = {
            "col_a": "Col A",
            "col_b": "Col B"
        }

        result = extractor.validate_columns_dataframe(df, mapping)

        assert len(result.columns) == 2
        assert "col_a" in result.columns
        assert "col_b" in result.columns

    def test_preserves_data(self, extractor):
        """Verifica que preserve los datos correctamente."""
        df = pd.DataFrame({
            "Valor": [100, 200, 300],
            "Texto": ["x", "y", "z"]
        })
        mapping = {
            "valor_db": "Valor",
            "texto_db": "Texto"
        }

        result = extractor.validate_columns_dataframe(df, mapping)

        assert list(result["valor_db"]) == [100, 200, 300]
        assert list(result["texto_db"]) == ["x", "y", "z"]

    def test_empty_dataframe(self, extractor):
        """Validación de DataFrame vacío pero con columnas correctas."""
        df = pd.DataFrame({"Col A": [], "Col B": []})
        mapping = {"a": "Col A", "b": "Col B"}

        result = extractor.validate_columns_dataframe(df, mapping)

        assert result is not None
        assert len(result) == 0
        assert list(result.columns) == ["a", "b"]


class TestProperties:
    """Tests para propiedades de acceso"""

    def test_conn_property(self, extractor):
        """Propiedad conn debe dar acceso a configuración de conexión."""
        assert extractor.conn.host == "sftp.example.com"
        assert extractor.conn.port == 22
        assert extractor.conn.username == "test_user"

    def test_paths_property(self, extractor):
        """Propiedad paths debe dar acceso a configuración de rutas."""
        assert extractor.paths.remote_dir == "/remote/input"
        assert extractor.paths.local_dir == "/tmp/test_output"
