# Tests para Correcciones de Bugs Críticos

Este directorio contiene tests exhaustivos para validar las correcciones de 12 bugs críticos identificados y corregidos en el proyecto.

## Estructura de Tests

```
tests/
├── conftest.py                           # Fixtures y mocks compartidos
├── unit/
│   ├── test_fase1_runtime_errors.py     # Tests para bugs #3, #4, #6, #8
│   ├── test_fase2_resource_leaks.py     # Tests para bugs #1, #2, #5, #7
│   └── test_fase3_seguridad_validacion.py  # Tests para bugs #9, #10, #11, #12
└── README_TESTS.md                       # Este archivo
```

## Instalación de Dependencias

```bash
pip install -r requirements-test.txt
```

## Ejecutar Tests

### Ejecutar todos los tests
```bash
pytest tests/ -v
```

### Ejecutar tests de una fase específica
```bash
# Fase 1: Runtime Errors
pytest tests/unit/test_fase1_runtime_errors.py -v

# Fase 2: Resource Leaks
pytest tests/unit/test_fase2_resource_leaks.py -v

# Fase 3: Seguridad y Validación
pytest tests/unit/test_fase3_seguridad_validacion.py -v
```

### Ejecutar tests con cobertura
```bash
pytest tests/ --cov=proyectos/energiafacilities --cov-report=html --cov-report=term
```

### Ejecutar tests en paralelo (más rápido)
```bash
pytest tests/ -n auto
```

### Ejecutar un test específico
```bash
pytest tests/unit/test_fase1_runtime_errors.py::TestBug3_ParametroCSV::test_load_data_csv_parametro_correcto -v
```

## Descripción de Tests por Fase

### FASE 1: Runtime Errors

#### BUG #3: Parámetro incorrecto en pd.read_csv
- **Archivo:** `test_fase1_runtime_errors.py::TestBug3_ParametroCSV`
- **Tests:**
  - `test_load_data_con_csv_sin_saltar_filas`: Verifica carga CSV sin skiprows
  - `test_load_data_con_csv_saltando_filas`: Verifica uso correcto de skiprows
  - `test_load_data_csv_parametro_correcto`: Valida que se usa parámetro nombrado

#### BUG #4: Indentación en base_postgress.py
- **Archivo:** `test_fase1_runtime_errors.py::TestBug4_IndentacionPostgres`
- **Tests:**
  - `test_ejecutar_sin_resultados_retorna_dataframe_vacio`: Valida que no hay UnboundLocalError
  - `test_ejecutar_con_resultados_retorna_dataframe_con_datos`: Verifica retorno con datos
  - `test_ejecutar_sp_sin_retorno_no_falla`: Valida stored procedures sin retorno

#### BUG #6: Inconsistencia de tipo en DAG
- **Archivo:** `test_fase1_runtime_errors.py::TestBug6_InconsistenciaTipoDag`
- **Tests:**
  - `test_extraersftp_retorna_dict_completo`: Verifica retorno de dict completo
  - `test_dag_puede_acceder_a_ruta_desde_dict`: Simula acceso del DAG

#### BUG #8: Parámetros faltantes
- **Archivo:** `test_fase1_runtime_errors.py::TestBug8_ParametrosFaltantes`
- **Tests:**
  - `test_verificar_datos_con_table_name`: Verifica que acepta table_name
  - `test_load_data_con_table_name`: Verifica carga con table_name
  - `test_loader_clienteslibres_pasa_table_name`: Valida paso de parámetro

---

### FASE 2: Resource Leaks

#### BUG #1: Conexiones SFTP sin cerrar
- **Archivo:** `test_fase2_resource_leaks.py::TestBug1_ConexionesSFTPSinCerrar`
- **Tests:**
  - `test_listar_archivos_cierra_conexion_en_exito`: Verifica cierre en éxito
  - `test_listar_archivos_cierra_conexion_en_error`: Verifica cierre con excepción
  - `test_listar_archivos_atributos_cierra_conexion_en_exito`: Valida cierre
  - `test_listar_archivos_atributos_cierra_conexion_en_error`: Valida cierre en error
  - `test_extract_cierra_conexion_en_exito`: Verifica cierre en extract()
  - `test_extract_cierra_conexion_en_error`: Verifica cierre con error

#### BUG #2: Transport SFTP sin cerrar
- **Archivo:** `test_fase2_resource_leaks.py::TestBug2_TransportSFTPSinCerrar`
- **Tests:**
  - `test_conectar_sftp_cierra_transport_en_error_connect`: Valida cierre si falla connect
  - `test_conectar_sftp_cierra_transport_en_error_sftp_client`: Valida cierre si falla client
  - `test_conectar_sftp_no_cierra_transport_en_exito`: Verifica que no cierra en éxito

#### BUG #5: Conexión PostgreSQL sin cerrar
- **Archivo:** `test_fase2_resource_leaks.py::TestBug5_ConexionPostgreSQLSinCerrar`
- **Tests:**
  - `test_run_sp_cierra_conexion_con_context_manager`: Verifica uso de context manager
  - `test_run_sp_cierra_conexion_incluso_con_error`: Valida cierre con error

#### BUG #7: Workbook Excel sin cerrar
- **Archivo:** `test_fase2_resource_leaks.py::TestBug7_WorkbookExcelSinCerrar`
- **Tests:**
  - `test_procesar_excel_cierra_workbook_en_exito`: Verifica cierre en éxito
  - `test_procesar_excel_cierra_workbook_en_error`: Verifica cierre con error
  - `test_procesar_excel_cierra_workbook_con_return_temprano`: Valida cierre con return

---

### FASE 3: Seguridad y Validación

#### BUG #10: SQL Injection potencial
- **Archivo:** `test_fase3_seguridad_validacion.py::TestBug10_SQLInjectionPrevencion`
- **Tests:**
  - `test_validate_sql_identifier_acepta_identificadores_validos`: Valida identificadores correctos
  - `test_validate_sql_identifier_rechaza_identificadores_invalidos`: Rechaza SQL injection
  - `test_validate_sql_identifier_rechaza_identificadores_vacios`: Rechaza vacíos
  - `test_validate_sql_identifier_rechaza_multiples_puntos`: Rechaza múltiples puntos
  - `test_validate_sql_identifier_rechaza_caracteres_especiales`: Rechaza caracteres peligrosos
  - `test_extract_valida_schema_y_table`: Verifica validación en extract()
  - `test_extract_valida_columnas`: Verifica validación de columnas
  - `test_insert_dataframe_valida_identificadores`: Verifica validación en insert
  - `test_insert_dataframe_valida_nombres_columnas_df`: Verifica validación de columnas DF

#### BUG #11: Validación de filename
- **Archivo:** `test_fase3_seguridad_validacion.py::TestBug11_ValidacionFilename`
- **Tests:**
  - `test_ejecutar_transformacion_rechaza_filename_none`: Rechaza filename None
  - `test_ejecutar_transformacion_acepta_filename_valido`: Acepta filename válido
  - `test_ejecutar_transformacion_usa_newdestinationoptional`: Usa alternativa

#### BUG #12: Validación SFTP rename
- **Archivo:** `test_fase3_seguridad_validacion.py::TestBug12_ValidacionSFTPRename`
- **Tests:**
  - `test_extract_advierte_si_archivo_destino_existe`: Verifica warning
  - `test_extract_no_advierte_si_archivo_destino_no_existe`: No advierte si no existe
  - `test_extract_continua_con_rename_despues_de_validacion`: Continúa después de validar

#### BUG #9: Variable no usada
- **Archivo:** `test_fase3_seguridad_validacion.py::TestBug9_VariableNoUsada`
- **Tests:**
  - `test_asegurar_directorio_sftp_no_asigna_variable_innecesaria`: Verifica limpieza
  - `test_asegurar_directorio_sftp_crea_directorios_faltantes`: Verifica funcionalidad

---

## Métricas de Tests

- **Total de tests:** ~45 tests
- **Bugs cubiertos:** 12/12 (100%)
- **Clases de test:** 12 clases
- **Cobertura esperada:** >90% de las correcciones realizadas

## Interpretación de Resultados

### Tests Exitosos
Si todos los tests pasan, significa que:
- ✅ No hay regresiones en las correcciones
- ✅ Los resource leaks están resueltos
- ✅ Las validaciones de seguridad funcionan
- ✅ El código es estable y robusto

### Tests Fallidos
Si algún test falla:
1. Revisar el mensaje de error detallado
2. Verificar que las dependencias estén instaladas
3. Comprobar que no se han modificado las correcciones
4. Revisar el stack trace para identificar la causa

## Ejecución en CI/CD

Para integrar en pipeline de CI/CD:

```yaml
# Ejemplo para GitHub Actions
- name: Run tests
  run: |
    pip install -r requirements-test.txt
    pytest tests/ --cov --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
  with:
    files: ./coverage.xml
```

## Mantenimiento de Tests

- Actualizar tests cuando se modifiquen las correcciones
- Agregar nuevos tests para nuevas funcionalidades
- Mantener cobertura >90%
- Ejecutar tests antes de cada commit

---

**Última actualización:** 2025-11-05
**Autor:** Claude Code
**Versión:** 1.0
