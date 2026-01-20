"""
Tests para mapeo de columnas en BaseLoaderPostgres.
Verifica la lógica de transformación Excel -> DB.
"""
import pytest
import pandas as pd
from core.base_loader import BaseLoaderPostgres


@pytest.fixture
def loader(postgres_config, loader_config):
    """Instancia de loader para tests."""
    return BaseLoaderPostgres(postgres_config, loader_config)


class TestApplyColumnMapping:
    """Tests para _apply_column_mapping()"""

    def test_basic_mapping(self, loader):
        """Mapeo simple de una columna."""
        df = pd.DataFrame({
            "Columna Excel": [1, 2, 3],
            "Otra Columna": [4, 5, 6]
        })
        # Mapeo invertido: DB -> Excel
        mapping = {"columna_db": "Columna Excel"}

        result = loader._apply_column_mapping(df, mapping)

        assert "columna_db" in result.columns
        assert "Otra Columna" not in result.columns
        assert len(result.columns) == 1
        assert list(result["columna_db"]) == [1, 2, 3]

    def test_multiple_columns_mapping(self, loader):
        """Mapeo de múltiples columnas."""
        df = pd.DataFrame({
            "Nombre": ["Ana", "Luis"],
            "Edad": [25, 30],
            "Ciudad": ["Lima", "Cusco"]
        })
        mapping = {
            "nombre_cliente": "Nombre",
            "edad_cliente": "Edad"
        }

        result = loader._apply_column_mapping(df, mapping)

        assert list(result.columns) == ["nombre_cliente", "edad_cliente"]
        assert len(result) == 2

    def test_case_insensitive_mapping(self, loader):
        """Mapeo debe funcionar sin importar mayúsculas/minúsculas."""
        df = pd.DataFrame({"COLUMNA EXCEL": [1, 2]})
        mapping = {"columna_db": "columna excel"}

        result = loader._apply_column_mapping(df, mapping)

        assert "columna_db" in result.columns

    def test_case_insensitive_mixed(self, loader):
        """Mapeo con mayúsculas mixtas."""
        df = pd.DataFrame({"CoLuMnA ExCeL": [1]})
        mapping = {"col_db": "COLUMNA EXCEL"}

        result = loader._apply_column_mapping(df, mapping)

        assert "col_db" in result.columns

    def test_strips_whitespace(self, loader):
        """Mapeo debe ignorar espacios extra."""
        df = pd.DataFrame({"  Columna  ": [1, 2]})
        mapping = {"col_db": "Columna"}

        result = loader._apply_column_mapping(df, mapping)

        assert "col_db" in result.columns

    def test_no_matching_columns_raises(self, loader):
        """Debe fallar si ninguna columna coincide."""
        df = pd.DataFrame({"col_a": [1], "col_b": [2]})
        mapping = {"db_col": "col_inexistente"}

        with pytest.raises(ValueError, match="ninguna columna coincide"):
            loader._apply_column_mapping(df, mapping)

    def test_partial_mapping(self, loader):
        """Mapeo parcial: solo algunas columnas del mapeo existen."""
        df = pd.DataFrame({
            "Columna A": [1],
            "Columna B": [2],
            "Columna C": [3]
        })
        mapping = {
            "col_a": "Columna A",
            "col_b": "Columna B",
            "col_d": "Columna D"  # No existe en df
        }

        result = loader._apply_column_mapping(df, mapping)

        # Solo mapea las que existen
        assert "col_a" in result.columns
        assert "col_b" in result.columns
        assert "col_d" not in result.columns

    def test_preserves_data_types(self, loader):
        """Mapeo debe preservar tipos de datos."""
        df = pd.DataFrame({
            "entero": [1, 2, 3],
            "flotante": [1.1, 2.2, 3.3],
            "texto": ["a", "b", "c"]
        })
        mapping = {
            "int_col": "entero",
            "float_col": "flotante",
            "str_col": "texto"
        }

        result = loader._apply_column_mapping(df, mapping)

        assert result["int_col"].dtype in [int, "int64"]
        assert result["float_col"].dtype in [float, "float64"]
        assert result["str_col"].dtype == object

    def test_empty_dataframe(self, loader):
        """Mapeo de DataFrame vacío pero con columnas."""
        df = pd.DataFrame({"Columna A": []})
        mapping = {"col_a": "Columna A"}

        result = loader._apply_column_mapping(df, mapping)

        assert "col_a" in result.columns
        assert len(result) == 0


class TestReadDataSource:
    """Tests para _read_data_source()"""

    def test_accepts_dataframe(self, loader):
        """Debe aceptar DataFrame directamente."""
        df = pd.DataFrame({"col": [1, 2, 3]})

        result, source = loader._read_data_source(df)

        assert isinstance(result, pd.DataFrame)
        assert source == "DataFrame"
        assert len(result) == 3

    def test_rejects_unsupported_format(self, loader):
        """Debe rechazar formatos no soportados."""
        with pytest.raises(ValueError, match="Formato no soportado"):
            loader._read_data_source(12345)

        with pytest.raises(ValueError, match="Formato no soportado"):
            loader._read_data_source(["lista", "de", "datos"])


class TestBaseLoaderInit:
    """Tests para inicialización de BaseLoaderPostgres"""

    def test_valid_config(self, postgres_config, loader_config):
        """Inicialización con config válida."""
        loader = BaseLoaderPostgres(postgres_config, loader_config)

        assert loader._cfg.host == "localhost"
        assert loader._cfgload.schema == "raw"

    def test_invalid_config_type(self):
        """Debe rechazar config que no sea dict."""
        with pytest.raises(ValueError, match="config debe ser un dict"):
            BaseLoaderPostgres("no es dict", {})

        with pytest.raises(ValueError, match="configload debe ser un dict"):
            BaseLoaderPostgres({}, "no es dict")

    def test_rejects_none_config(self):
        """Debe rechazar None como config."""
        with pytest.raises(ValueError):
            BaseLoaderPostgres(None, {})
