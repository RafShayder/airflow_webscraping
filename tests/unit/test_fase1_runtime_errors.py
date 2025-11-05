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
from core.base_loader import BaseLoaderPostgres
from core.base_postgress import PostgresConnector


class TestBug3_ParametroCSV:
    """
    BUG #3: Parámetro incorrecto en pd.read_csv
    Verificar que pd.read_csv usa skiprows correctamente
    """

    def test_load_data_con_csv_sin_saltar_filas(self, postgres_config, loader_config, sample_csv_file, mock_postgres_connection):
        """Test carga CSV sin saltar filas"""
        with patch('core.base_loader.psycopg2.connect', return_value=mock_postgres_connection):
            loader = BaseLoaderPostgres(postgres_config, loader_config)

            # Debe poder cargar CSV correctamente
            result = loader.load_data(
                data=sample_csv_file,
                numerofilasalto=0
            )

            assert result['status'] == 'success'

    def test_load_data_con_csv_saltando_filas(self, postgres_config, loader_config, temp_dir, mock_postgres_connection):
        """Test carga CSV saltando primeras N filas"""
        # Crear CSV con filas a saltar
        csv_path = os.path.join(temp_dir, 'test_skip.csv')
        with open(csv_path, 'w') as f:
            f.write("# Comentario línea 1\n")
            f.write("# Comentario línea 2\n")
            f.write("id,nombre,edad\n")
            f.write("1,Juan,25\n")
            f.write("2,María,30\n")

        with patch('core.base_loader.psycopg2.connect', return_value=mock_postgres_connection):
            loader = BaseLoaderPostgres(postgres_config, loader_config)

            # Debe saltar las primeras 2 líneas usando skiprows (no posicional)
            result = loader.load_data(
                data=csv_path,
                numerofilasalto=2
            )

            assert result['status'] == 'success'

    def test_load_data_csv_parametro_correcto(self, postgres_config, loader_config, sample_csv_file):
        """Test que verifica que se usa el parámetro nombrado 'skiprows'"""
        with patch('core.base_loader.psycopg2.connect') as mock_connect, \
             patch('core.base_loader.pd.read_csv') as mock_read_csv:

            mock_connect.return_value.__enter__ = Mock()
            mock_connect.return_value.__exit__ = Mock()
            mock_read_csv.return_value = pd.DataFrame({'id': [1], 'nombre': ['Test']})

            loader = BaseLoaderPostgres(postgres_config, loader_config)

            loader.load_data(data=sample_csv_file, numerofilasalto=3)

            # Verificar que pd.read_csv fue llamado con skiprows= (no posicional)
            mock_read_csv.assert_called_once()
            call_kwargs = mock_read_csv.call_args[1]
            assert 'skiprows' in call_kwargs
            assert call_kwargs['skiprows'] == 3


class TestBug4_IndentacionPostgres:
    """
    BUG #4: Indentación incorrecta en base_postgress.py
    Verificar que ejecutar() siempre retorna un DataFrame válido
    """

    def test_ejecutar_sin_resultados_retorna_dataframe_vacio(self, postgres_config):
        """Test que ejecutar() retorna DataFrame vacío cuando no hay resultados"""
        with patch('core.base_postgress.create_engine') as mock_engine:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()

            # Simular stored procedure sin resultados (cur.description = None)
            mock_cursor.description = None
            mock_cursor.fetchall.return_value = []

            mock_conn.cursor.return_value = mock_cursor
            mock_engine.return_value.raw_connection.return_value = mock_conn

            pg = PostgresConnector(postgres_config)

            # No debe lanzar UnboundLocalError
            result = pg.ejecutar("public.test_sp", tipo='sp')

            assert isinstance(result, pd.DataFrame)
            assert result.empty

    def test_ejecutar_con_resultados_retorna_dataframe_con_datos(self, postgres_config):
        """Test que ejecutar() retorna DataFrame con datos cuando hay resultados"""
        with patch('core.base_postgress.create_engine') as mock_engine:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()

            # Simular query con resultados
            mock_cursor.description = [('id',), ('nombre',)]
            mock_cursor.fetchall.return_value = [(1, 'Juan'), (2, 'María')]

            mock_conn.cursor.return_value = mock_cursor
            mock_engine.return_value.raw_connection.return_value = mock_conn

            pg = PostgresConnector(postgres_config)

            result = pg.ejecutar("SELECT * FROM test", tipo='query')

            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert len(result) == 2
            assert list(result.columns) == ['id', 'nombre']

    def test_ejecutar_sp_sin_retorno_no_falla(self, postgres_config):
        """Test que SP sin retorno no causa UnboundLocalError"""
        with patch('core.base_postgress.create_engine') as mock_engine:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()

            # SP que no retorna nada
            mock_cursor.description = None

            mock_conn.cursor.return_value = mock_cursor
            mock_engine.return_value.raw_connection.return_value = mock_conn

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

    def test_extraersftp_retorna_dict_completo(self, sftp_config_connect, sftp_config_paths):
        """Test que extraersftp_clienteslibres retorna dict con 'ruta'"""
        from sources.clientes_libres.stractor import extraersftp_clienteslibres

        with patch('sources.clientes_libres.stractor.load_config') as mock_config, \
             patch('sources.clientes_libres.stractor.BaseExtractorSFTP') as mock_extractor_class:

            mock_config.return_value = {
                'sftp_daas_c': sftp_config_connect,
                'clientes_libres': {**sftp_config_paths, 'specific_filename': 'test-file'}
            }

            mock_extractor = MagicMock()
            mock_extractor.extract.return_value = {
                'status': 'success',
                'code': 200,
                'etl_msg': 'OK',
                'ruta': '/tmp/test_file.xlsx'
            }
            mock_extractor_class.return_value = mock_extractor

            result = extraersftp_clienteslibres()

            # Debe retornar dict completo, no solo string
            assert isinstance(result, dict)
            assert 'ruta' in result
            assert result['ruta'] == '/tmp/test_file.xlsx'
            assert 'status' in result

    def test_dag_puede_acceder_a_ruta_desde_dict(self, sftp_config_connect, sftp_config_paths):
        """Test que simula el acceso del DAG a resultado_extract.get('ruta')"""
        from sources.clientes_libres.stractor import extraersftp_clienteslibres

        with patch('sources.clientes_libres.stractor.load_config') as mock_config, \
             patch('sources.clientes_libres.stractor.BaseExtractorSFTP') as mock_extractor_class:

            mock_config.return_value = {
                'sftp_daas_c': sftp_config_connect,
                'clientes_libres': {**sftp_config_paths, 'specific_filename': 'test-file'}
            }

            mock_extractor = MagicMock()
            mock_extractor.extract.return_value = {
                'ruta': '/tmp/test_file.xlsx',
                'status': 'success'
            }
            mock_extractor_class.return_value = mock_extractor

            resultado_extract = extraersftp_clienteslibres()

            # Simular código del DAG
            path_extraido = resultado_extract.get("ruta") if isinstance(resultado_extract, dict) else resultado_extract

            # No debe fallar y debe obtener la ruta
            assert path_extraido == '/tmp/test_file.xlsx'


class TestBug8_ParametrosFaltantes:
    """
    BUG #8: Parámetros faltantes en verificar_datos
    Verificar que se pasa table_name correctamente
    """

    def test_verificar_datos_con_table_name(self, postgres_config, loader_config, sample_excel_file, mock_postgres_connection):
        """Test que verificar_datos recibe table_name"""
        with patch('core.base_loader.psycopg2.connect', return_value=mock_postgres_connection):
            loader = BaseLoaderPostgres(postgres_config, loader_config)

            # Debe aceptar table_name sin error
            result = loader.verificar_datos(
                data=sample_excel_file,
                table_name='custom_table',
                strictreview=False
            )

            assert result['status'] == 'success'

    def test_load_data_con_table_name(self, postgres_config, loader_config, sample_excel_file, mock_postgres_connection):
        """Test que load_data usa table_name correctamente"""
        with patch('core.base_loader.psycopg2.connect', return_value=mock_postgres_connection):
            loader = BaseLoaderPostgres(postgres_config, loader_config)

            # Debe poder cargar con table_name especificado
            result = loader.load_data(
                data=sample_excel_file,
                table_name='custom_table'
            )

            assert result['status'] == 'success'

    def test_loader_clienteslibres_pasa_table_name(self, postgres_config):
        """Test que load_clienteslibres pasa table_name a verificar_datos y load_data"""
        from sources.clientes_libres.loader import load_clienteslibres

        with patch('sources.clientes_libres.loader.load_config') as mock_config, \
             patch('sources.clientes_libres.loader.BaseLoaderPostgres') as mock_loader_class:

            mock_config.return_value = {
                'postgress': postgres_config,
                'clientes_libres': {
                    'local_destination_dir': '/tmp/test.xlsx',
                    'table': 'clientes_table',
                    'schema': 'public'
                }
            }

            mock_loader = MagicMock()
            mock_loader.verificar_datos.return_value = {'status': 'success'}
            mock_loader.load_data.return_value = {'status': 'success', 'code': 200}
            mock_loader_class.return_value = mock_loader

            result = load_clienteslibres(filepath='/tmp/test.xlsx')

            # Verificar que se llamó verificar_datos con table_name
            assert mock_loader.verificar_datos.called
            call_kwargs = mock_loader.verificar_datos.call_args[1]
            assert 'table_name' in call_kwargs

            # Verificar que se llamó load_data con table_name
            assert mock_loader.load_data.called
            call_kwargs = mock_loader.load_data.call_args[1]
            assert 'table_name' in call_kwargs


# Ejecutar tests si se ejecuta directamente
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
