"""
Tests para validación de identificadores SQL.
Crítico: Previene SQL injection en nombres de tablas/columnas.
"""
import pytest
from core.base_loader import (
    _validate_sql_identifier,
    _strip_quotes,
    _validate_identifier_chars,
    _escape_identifier_if_needed
)


class TestStripQuotes:
    """Tests para _strip_quotes()"""

    def test_removes_surrounding_quotes(self):
        assert _strip_quotes('"tabla"') == "tabla"

    def test_keeps_unquoted_string(self):
        assert _strip_quotes("tabla") == "tabla"

    def test_keeps_partial_quotes(self):
        # Solo remueve si tiene ambas comillas
        assert _strip_quotes('"tabla') == '"tabla'
        assert _strip_quotes('tabla"') == 'tabla"'


class TestValidateIdentifierChars:
    """Tests para _validate_identifier_chars()"""

    def test_valid_alphanumeric(self):
        # No debe lanzar excepción
        _validate_identifier_chars("tabla_123")
        _validate_identifier_chars("TABLA")
        _validate_identifier_chars("t")

    def test_rejects_internal_quotes(self):
        with pytest.raises(ValueError, match="comillas dobles"):
            _validate_identifier_chars('tabla"inyeccion')

    def test_rejects_special_chars(self):
        with pytest.raises(ValueError, match="caracteres no permitidos"):
            _validate_identifier_chars("tabla-con-guiones")

        with pytest.raises(ValueError, match="caracteres no permitidos"):
            _validate_identifier_chars("tabla con espacios")

        with pytest.raises(ValueError, match="caracteres no permitidos"):
            _validate_identifier_chars("tabla;drop")


class TestEscapeIdentifierIfNeeded:
    """Tests para _escape_identifier_if_needed()"""

    def test_escapes_numeric_start(self):
        assert _escape_identifier_if_needed("45_min_voltaje") == '"45_min_voltaje"'
        assert _escape_identifier_if_needed("123abc") == '"123abc"'

    def test_keeps_alpha_start(self):
        assert _escape_identifier_if_needed("tabla_45") == "tabla_45"
        assert _escape_identifier_if_needed("a123") == "a123"


class TestValidateSqlIdentifier:
    """Tests para _validate_sql_identifier() - función principal"""

    # === Casos válidos ===

    def test_valid_simple_identifier(self):
        assert _validate_sql_identifier("users") == "users"
        assert _validate_sql_identifier("tabla_123") == "tabla_123"
        assert _validate_sql_identifier("MAYUSCULAS") == "MAYUSCULAS"

    def test_valid_schema_table(self):
        assert _validate_sql_identifier("raw.facturas") == "raw.facturas"
        assert _validate_sql_identifier("ods.dim_cliente") == "ods.dim_cliente"

    def test_identifier_starting_with_number(self):
        # PostgreSQL requiere comillas para identificadores que empiezan con número
        assert _validate_sql_identifier("45_min_voltaje") == '"45_min_voltaje"'
        assert _validate_sql_identifier("123_tabla") == '"123_tabla"'

    def test_schema_table_with_numeric_table(self):
        # Schema normal, tabla con número
        result = _validate_sql_identifier("raw.45_mediciones")
        assert result == 'raw."45_mediciones"'

    def test_already_quoted_identifier(self):
        # Debe remover comillas y re-validar
        assert _validate_sql_identifier('"tabla"') == "tabla"
        assert _validate_sql_identifier('"45_min"') == '"45_min"'

    # === Casos de SQL injection ===

    def test_rejects_semicolon_injection(self):
        with pytest.raises(ValueError):
            _validate_sql_identifier("users; DROP TABLE users;--")

    def test_rejects_quote_injection(self):
        with pytest.raises(ValueError):
            _validate_sql_identifier("tabla' OR '1'='1")

    def test_rejects_comment_injection(self):
        with pytest.raises(ValueError):
            _validate_sql_identifier("tabla--comentario")

    def test_rejects_union_injection(self):
        with pytest.raises(ValueError):
            _validate_sql_identifier("tabla UNION SELECT")

    # === Casos de caracteres inválidos ===

    def test_rejects_hyphen(self):
        with pytest.raises(ValueError, match="caracteres no permitidos"):
            _validate_sql_identifier("tabla-con-guiones")

    def test_rejects_spaces(self):
        with pytest.raises(ValueError, match="caracteres no permitidos"):
            _validate_sql_identifier("tabla con espacios")

    def test_rejects_parentheses(self):
        with pytest.raises(ValueError):
            _validate_sql_identifier("tabla()")

    def test_rejects_asterisk(self):
        with pytest.raises(ValueError):
            _validate_sql_identifier("*")

    # === Casos de estructura inválida ===

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError, match="no puede estar vacío"):
            _validate_sql_identifier("")

    def test_rejects_too_many_dots(self):
        with pytest.raises(ValueError, match="demasiados puntos"):
            _validate_sql_identifier("schema.tabla.columna")

        with pytest.raises(ValueError, match="demasiados puntos"):
            _validate_sql_identifier("a.b.c.d")

    # === Casos edge ===

    def test_single_character(self):
        assert _validate_sql_identifier("a") == "a"

    def test_underscore_only(self):
        assert _validate_sql_identifier("_") == "_"
        assert _validate_sql_identifier("___") == "___"

    def test_mixed_case(self):
        assert _validate_sql_identifier("TablaConMayusculas") == "TablaConMayusculas"
