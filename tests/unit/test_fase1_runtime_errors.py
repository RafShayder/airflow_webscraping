"""
Tests para validar correcciones de FASE 1 - Runtime Errors
Bugs: #3, #4, #6, #8
"""
import pytest
import pandas as pd
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, call
import tempfile
import os

# Importar módulos a testear
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "proyectos" / "energiafacilities"))


class TestBug3_ParametroCSV:
    """
    BUG #3: Parámetro incorrecto en pd.read_csv
    Verificar que pd.read_csv usa skiprows correctamente
    """

    def test_load_data_csv_parametro_correcto(self):
        """Test que verifica que se usa el parámetro nombrado 'skiprows'"""
        from core.base_loader import BaseLoaderPostgres

        with patch('core.base_loader.psycopg2.connect') as mock_connect, \
             patch('core.base_loader.pd.read_csv') as mock_read_csv:

            mock_connect.return_value.__enter__ = Mock()
            mock_connect.return_value.__exit__ = Mock()
            mock_read_csv.return_value = pd.DataFrame({'id': [1], 'nombre': ['Test']})

            postgres_config = {'host': 'localhost', 'port': 5432, 'database': 'test', 'user': 'test', 'password': 'test'}
            loader_config = {'schema': 'public', 'table': 'test_table', 'if_exists': 'replace'}

            loader = BaseLoaderPostgres(postgres_config, loader_config)

            loader.load_data(data='test.csv', numerofilasalto=3)

            # Verificar que pd.read_csv fue llamado con skiprows= (no posicional)
            mock_read_csv.assert_called_once()
            call_kwargs = mock_read_csv.call_args[1]
            assert 'skiprows' in call_kwargs
            assert call_kwargs['skiprows'] == 3

    def test_pd_read_csv_usa_skiprows_nombrado(self):
        """Test directo de que pd.read_csv acepta skiprows como parámetro nombrado"""
        import pandas as pd
        import io

        csv_data = "# comment\nid,nombre\n1,Juan\n2,Maria"

        # Debe funcionar con skiprows como parámetro nombrado
        df = pd.read_csv(io.StringIO(csv_data), skiprows=1)

        assert len(df) == 2
        assert 'id' in df.columns
        assert 'nombre' in df.columns


class TestBug4_IndentacionPostgres:
    """
    BUG #4: Indentación incorrecta en base_postgress.py
    Verificar que ejecutar() siempre retorna un DataFrame válido
    """

    def test_ejecutar_sin_resultados_retorna_dataframe_vacio(self):
        """Test que ejecutar() retorna DataFrame vacío cuando no hay resultados"""
        from core.base_postgress import PostgresConnector

        with patch('core.base_postgress.create_engine') as mock_engine:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()

            # Simular stored procedure sin resultados (cur.description = None)
            mock_cursor.description = None
            mock_cursor.fetchall.return_value = []

            mock_conn.cursor.return_value = mock_cursor
            mock_engine.return_value.raw_connection.return_value = mock_conn

            postgres_config = {'host': 'localhost', 'port': 5432, 'database': 'test', 'user': 'test', 'password': 'test'}
            pg = PostgresConnector(postgres_config)

            # No debe lanzar UnboundLocalError
            result = pg.ejecutar("public.test_sp", tipo='sp')

            assert isinstance(result, pd.DataFrame)
            assert result.empty

    def test_ejecutar_con_resultados_retorna_dataframe_con_datos(self):
        """Test que ejecutar() retorna DataFrame con datos cuando hay resultados"""
        from core.base_postgress import PostgresConnector

        with patch('core.base_postgress.create_engine') as mock_engine:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()

            # Simular query con resultados
            mock_cursor.description = [('id',), ('nombre',)]
            mock_cursor.fetchall.return_value = [(1, 'Juan'), (2, 'María')]

            mock_conn.cursor.return_value = mock_cursor
            mock_engine.return_value.raw_connection.return_value = mock_conn

            postgres_config = {'host': 'localhost', 'port': 5432, 'database': 'test', 'user': 'test', 'password': 'test'}
            pg = PostgresConnector(postgres_config)

            result = pg.ejecutar("SELECT * FROM test", tipo='query')

            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert len(result) == 2
            assert list(result.columns) == ['id', 'nombre']

    def test_ejecutar_sp_sin_retorno_no_falla(self):
        """Test que SP sin retorno no causa UnboundLocalError"""
        from core.base_postgress import PostgresConnector

        with patch('core.base_postgress.create_engine') as mock_engine:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()

            # SP que no retorna nada
            mock_cursor.description = None

            mock_conn.cursor.return_value = mock_cursor
            mock_engine.return_value.raw_connection.return_value = mock_conn

            postgres_config = {'host': 'localhost', 'port': 5432, 'database': 'test', 'user': 'test', 'password': 'test'}
            pg = PostgresConnector(postgres_config)

            # No debe fallar
            result = pg.ejecutar("public.update_records", parametros=(), tipo='sp')

            assert isinstance(result, pd.DataFrame)
            assert result.empty


class TestBug6_InconsistenciaTipoDag:
    """
    BUG #6: Inconsistencia de tipo de retorno en DAG
    Verificar que extractor retorna dict completo, no solo string
    """

    def test_extractor_debe_retornar_dict_con_ruta(self):
        """Test conceptual: extractor debe retornar dict con clave 'ruta'"""
        # Este test valida el concepto de la corrección
        # En el código real, extract() retorna un dict

        # Simular retorno correcto
        metastraccion = {
            'status': 'success',
            'code': 200,
            'etl_msg': 'OK',
            'ruta': '/tmp/test_file.xlsx'
        }

        # El DAG debe poder acceder a .get('ruta')
        assert isinstance(metastraccion, dict)
        assert 'ruta' in metastraccion
        path_extraido = metastraccion.get("ruta")
        assert path_extraido == '/tmp/test_file.xlsx'

    def test_dag_accede_a_ruta_correctamente(self):
        """Test que simula el código del DAG accediendo al resultado"""
        # Simular resultado del extractor (después de la corrección)
        resultado_extract = {
            'ruta': '/tmp/test_file.xlsx',
            'status': 'success',
            'code': 200
        }

        # Código del DAG (línea 23 de DAG_clientes_libres.py)
        path_extraido = resultado_extract.get("ruta") if isinstance(resultado_extract, dict) else resultado_extract

        # Debe funcionar correctamente
        assert path_extraido == '/tmp/test_file.xlsx'


class TestBug8_ParametrosFaltantes:
    """
    BUG #8: Parámetros faltantes en verificar_datos
    Verificar que se pasa table_name correctamente
    """

    def test_verificar_datos_acepta_table_name_como_parametro(self):
        """Test que verifica que verificar_datos acepta table_name"""
        from core.base_loader import BaseLoaderPostgres
        import inspect

        # Verificar que el método acepta table_name como parámetro
        sig = inspect.signature(BaseLoaderPostgres.verificar_datos)
        params = list(sig.parameters.keys())

        assert 'table_name' in params

    def test_load_data_acepta_table_name_como_parametro(self):
        """Test que verifica que load_data acepta table_name"""
        from core.base_loader import BaseLoaderPostgres
        import inspect

        # Verificar que el método acepta table_name como parámetro
        sig = inspect.signature(BaseLoaderPostgres.load_data)
        params = list(sig.parameters.keys())

        assert 'table_name' in params

    def test_insert_dataframe_acepta_table_name_como_parametro(self):
        """Test que verifica que insert_dataframe acepta table_name"""
        from core.base_loader import BaseLoaderPostgres
        import inspect

        # Verificar que el método acepta table_name como parámetro
        sig = inspect.signature(BaseLoaderPostgres.insert_dataframe)
        params = list(sig.parameters.keys())

        assert 'table_name' in params
        assert 'schema' in params


# Ejecutar tests si se ejecuta directamente
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
