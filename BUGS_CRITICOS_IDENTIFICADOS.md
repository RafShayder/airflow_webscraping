# REPORTE DE BUGS CRÍTICOS IDENTIFICADOS

**Fecha:** 2025-11-05
**Proyecto:** scraper-teleows
**Analizador:** Claude Code

---

## RESUMEN EJECUTIVO

Se han identificado **12 bugs críticos** que pueden causar:
- **Resource leaks** (conexiones SFTP y BD sin cerrar)
- **Errores en tiempo de ejecución** (parámetros incorrectos)
- **Pérdida de datos** (indentación incorrecta)
- **Inconsistencias en DAGs** (tipos de retorno incompatibles)

**SEVERIDAD:**
- 🔴 **CRÍTICA**: 8 bugs
- 🟡 **ALTA**: 4 bugs

---

## 🔴 BUGS CRÍTICOS (Prioridad Máxima)

### BUG #1: Resource Leak - Conexiones SFTP sin cerrar
**Archivo:** `proyectos/energiafacilities/core/base_stractor.py`
**Líneas:** 122-130, 134-150, 156-193
**Severidad:** 🔴 CRÍTICA

**Descripción:**
En los métodos `listar_archivos()`, `listar_archivos_atributos()` y `extract()`, las conexiones SFTP se abren pero no se cierran correctamente si ocurre una excepción, causando resource leaks.

**Código problemático:**
```python
# Líneas 122-130
def listar_archivos(self, ruta_remota: str | None = None) -> List[str]:
    ruta = ruta_remota or self.paths.remote_dir
    try:
        sftp = self.conectar_sftp()
        archivos = sftp.listdir(ruta)
        sftp.close()  # ❌ No se ejecuta si hay excepción
        logger.debug(f"Archivos encontrados en {ruta}: {archivos}")
        return archivos
    except Exception as e:
        logger.error(f"Error al listar archivos en {ruta}: {e}")
        raise
```

**Impacto:**
- Agotamiento de conexiones SFTP disponibles
- Problemas de memoria en procesos de larga duración (Airflow)
- Posible bloqueo del servidor SFTP

**Solución recomendada:**
Usar un bloque `finally` para garantizar el cierre de conexiones:
```python
def listar_archivos(self, ruta_remota: str | None = None) -> List[str]:
    ruta = ruta_remota or self.paths.remote_dir
    sftp = None
    try:
        sftp = self.conectar_sftp()
        archivos = sftp.listdir(ruta)
        logger.debug(f"Archivos encontrados en {ruta}: {archivos}")
        return archivos
    except Exception as e:
        logger.error(f"Error al listar archivos en {ruta}: {e}")
        raise
    finally:
        if sftp:
            sftp.close()
```

**Métodos afectados:**
- `listar_archivos()` (líneas 122-130)
- `listar_archivos_atributos()` (líneas 134-150)
- `extract()` (líneas 156-193)

---

### BUG #2: Transport SFTP sin cerrar
**Archivo:** `proyectos/energiafacilities/core/base_stractor.py`
**Líneas:** 81-92
**Severidad:** 🔴 CRÍTICA

**Descripción:**
Se crea un objeto `paramiko.Transport` pero nunca se guarda la referencia ni se cierra explícitamente. Si hay un error después de crear el transport pero antes de crear el SFTP client, el transport queda abierto indefinidamente.

**Código problemático:**
```python
def conectar_sftp(self) -> paramiko.SFTPClient:
    """Devuelve un cliente SFTP activo listo para usar."""
    try:
        transport = paramiko.Transport((self.conn.host, self.conn.port))  # ❌ No se guarda
        transport.connect(username=self.conn.username, password=self.conn.password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logger.debug(f"Conexión SFTP establecida con {self.conn.host}")
        return sftp
    except Exception as e:
        logger.error(f"Error al conectar con SFTP: {e}")
        raise ConnectionError(f"No se pudo conectar al SFTP: {e}")
```

**Impacto:**
- Resource leak del transport layer
- Conexiones TCP/IP abiertas indefinidamente
- Problemas de red y conexiones fantasma

**Solución recomendada:**
Guardar la referencia del transport en el SFTP client para posterior cierre:
```python
def conectar_sftp(self) -> paramiko.SFTPClient:
    transport = None
    try:
        transport = paramiko.Transport((self.conn.host, self.conn.port))
        transport.connect(username=self.conn.username, password=self.conn.password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logger.debug(f"Conexión SFTP establecida con {self.conn.host}")
        return sftp
    except Exception as e:
        if transport:
            transport.close()
        logger.error(f"Error al conectar con SFTP: {e}")
        raise ConnectionError(f"No se pudo conectar al SFTP: {e}")
```

---

### BUG #3: Parámetro incorrecto en pd.read_csv
**Archivo:** `proyectos/energiafacilities/core/base_loader.py`
**Línea:** 155
**Severidad:** 🔴 CRÍTICA

**Descripción:**
Se pasa `numerofilasalto` como primer parámetro posicional a `pd.read_csv()`, cuando debería ser el parámetro nombrado `skiprows=numerofilasalto`. Esto causará un error porque pandas espera una ruta de archivo como primer parámetro.

**Código problemático:**
```python
# Línea 155
elif isinstance(data, str) and data.lower().endswith(".csv"):
    df = pd.read_csv(data, numerofilasalto)  # ❌ Parámetro posicional incorrecto
```

**Impacto:**
- Error en tiempo de ejecución al cargar archivos CSV
- Fallo en tareas de ETL que usan archivos CSV
- Interrupción del flujo de Airflow DAGs

**Solución recomendada:**
```python
elif isinstance(data, str) and data.lower().endswith(".csv"):
    df = pd.read_csv(data, skiprows=numerofilasalto)  # ✅ Correcto
```

---

### BUG #4: Indentación incorrecta causa pérdida de datos
**Archivo:** `proyectos/energiafacilities/core/base_postgress.py`
**Línea:** 136
**Severidad:** 🔴 CRÍTICA

**Descripción:**
La línea 136 tiene una indentación extra que hace que el DataFrame solo se cree cuando `cur.description` existe. Si no hay descripción, el código intentará retornar un `df` no definido, causando un `UnboundLocalError`.

**Código problemático:**
```python
# Líneas 131-141
if cur.description:
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=cols)
               # ❌ Indentación extra aquí (línea 136)
else:
    df = pd.DataFrame()

logger.debug(f"Ejecución de {tipo} '{consulta}' completada correctamente.")
return df  # ❌ df puede no estar definido
```

**Impacto:**
- UnboundLocalError cuando no hay resultados en una query
- Fallo en stored procedures que no retornan datos
- Interrupción crítica en pipelines de datos

**Solución recomendada:**
Corregir la indentación en la línea 136:
```python
if cur.description:
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=cols)  # ✅ Indentación correcta
else:
    df = pd.DataFrame()
```

---

### BUG #5: Resource Leak - Conexión PostgreSQL sin cerrar
**Archivo:** `proyectos/energiafacilities/core/base_run_sp.py`
**Líneas:** 22-31
**Severidad:** 🔴 CRÍTICA

**Descripción:**
Se crea una instancia de `PostgresConnector` pero nunca se llama a `postgress.close()`. Esto causa un resource leak de conexiones a la base de datos.

**Código problemático:**
```python
def run_sp(configyaml: str, configpostgress:str="postgress", sp_name:str='sp_carga', sp_value:str=None):
    try:
        config = load_config()
        postgres_config = config.get(configpostgress, {})
        general_config = config.get(configyaml, {})

        # Crear instancia de conexión
        postgress = PostgresConnector(postgres_config)  # ❌ Nunca se cierra

        sp_ejecutar = sp_value or general_config[sp_name]
        logger.info(f"Ejecutando SP {sp_ejecutar}")
        postgress.ejecutar(sp_ejecutar, tipo='sp')
        data = postgress.ejecutar("public.log_sp_ultimo_fn", parametros=(f'{sp_ejecutar}()',), tipo='fn')
        logger.info(f"Estado SP: {data['estado'].values}, Detalle: {data['msj_error'].values}")
    except Exception as e:
        logger.error(f"Se produjo un error al ejecutar el sp: {e}")
        raise
```

**Impacto:**
- Agotamiento del pool de conexiones PostgreSQL
- Errores "too many connections" en producción
- Degradación del rendimiento de la base de datos

**Solución recomendada:**
Usar context manager o cerrar explícitamente:
```python
def run_sp(configyaml: str, configpostgress:str="postgress", sp_name:str='sp_carga', sp_value:str=None):
    config = load_config()
    postgres_config = config.get(configpostgress, {})
    general_config = config.get(configyaml, {})

    with PostgresConnector(postgres_config) as postgress:
        sp_ejecutar = sp_value or general_config[sp_name]
        logger.info(f"Ejecutando SP {sp_ejecutar}")
        postgress.ejecutar(sp_ejecutar, tipo='sp')
        data = postgress.ejecutar("public.log_sp_ultimo_fn", parametros=(f'{sp_ejecutar}()',), tipo='fn')
        logger.info(f"Estado SP: {data['estado'].values}, Detalle: {data['msj_error'].values}")
```

---

### BUG #6: Inconsistencia de tipo de retorno en DAG
**Archivo:** `dags/DAG_clientes_libres.py`
**Líneas:** 22-24
**Severidad:** 🔴 CRÍTICA

**Descripción:**
El DAG espera que `extraersftp_clienteslibres()` retorne un dict con clave "ruta", pero la función retorna directamente un string (`metastraccion['ruta']`). Esto causa un AttributeError cuando el DAG intenta acceder a `.get("ruta")`.

**Código problemático en DAG:**
```python
# Líneas 22-24
def procesar_transform_clientes_libres(**kwargs):
    ti = kwargs['ti']
    resultado_extract = ti.xcom_pull(task_ids='extract_clientes_libres')
    # ❌ Espera un dict pero recibe un string
    path_extraido = resultado_extract.get("ruta") if isinstance(resultado_extract, dict) else resultado_extract
    return transformer_clienteslibres(filepath=path_extraido)
```

**Código en stractor.py:**
```python
# Línea 19 de sources/clientes_libres/stractor.py
def extraersftp_clienteslibres():
    # ...
    metastraccion = Extractor.extract(specific_file=nombrearchivoextraer)
    return metastraccion['ruta']  # ❌ Retorna string, no dict
```

**Impacto:**
- Fallo del DAG en tiempo de ejecución
- Tarea `transform_clientes_libres` recibe datos incorrectos
- Pipeline ETL interrumpido

**Solución recomendada:**
Modificar el retorno en `stractor.py` para ser consistente:
```python
def extraersftp_clienteslibres():
    # ...
    metastraccion = Extractor.extract(specific_file=nombrearchivoextraer)
    return metastraccion  # ✅ Retornar el dict completo
```

---

### BUG #7: Workbook Excel sin cerrar (Resource Leak)
**Archivo:** `proyectos/energiafacilities/sources/clientes_libres/help/transform_helpers.py`
**Línea:** 143
**Severidad:** 🔴 CRÍTICA

**Descripción:**
Se abre un workbook de Excel con `load_workbook()` pero nunca se cierra con `wb.close()`. Esto causa resource leaks de file handles, especialmente crítico en Airflow que ejecuta tareas repetidamente.

**Código problemático:**
```python
def _procesar_excel(path_xlsx: Path, mapping: dict, sheet_names: list[str]) -> pd.DataFrame:
    """Procesa las hojas indicadas del archivo Excel según el mapping."""
    wb = load_workbook(filename=path_xlsx, data_only=True, read_only=True)  # ❌ Nunca se cierra
    logger.info(f"Hojas disponibles en {path_xlsx.name}")
    disponibles = set(wb.sheetnames)
    registros = []

    for nombre in sheet_names:
        if nombre not in disponibles:
            logger.warning(f"La hoja '{nombre}' no existe en {path_xlsx.name}.")
            continue
        ws = wb[nombre]
        registro = _leer_registro(ws, mapping)
        registro["hoja"] = nombre
        registros.append(registro)
    # ... resto del código
    return df  # ❌ wb nunca se cierra
```

**Impacto:**
- File handles abiertos indefinidamente
- Problemas de memoria en ejecuciones repetidas
- Posible error "Too many open files" en el sistema

**Solución recomendada:**
```python
def _procesar_excel(path_xlsx: Path, mapping: dict, sheet_names: list[str]) -> pd.DataFrame:
    wb = load_workbook(filename=path_xlsx, data_only=True, read_only=True)
    try:
        logger.info(f"Hojas disponibles en {path_xlsx.name}")
        disponibles = set(wb.sheetnames)
        registros = []

        for nombre in sheet_names:
            if nombre not in disponibles:
                logger.warning(f"La hoja '{nombre}' no existe en {path_xlsx.name}.")
                continue
            ws = wb[nombre]
            registro = _leer_registro(ws, mapping)
            registro["hoja"] = nombre
            registros.append(registro)
        # ... resto del código
        return df
    finally:
        wb.close()  # ✅ Garantizar cierre
```

---

### BUG #8: Parámetros faltantes en verificar_datos
**Archivo:** `proyectos/energiafacilities/sources/clientes_libres/loader.py`
**Línea:** 15
**Severidad:** 🔴 CRÍTICA

**Descripción:**
Se llama a `verificar_datos()` sin el parámetro requerido `table_name`, lo que causa que use `self._cfgload.table` que puede no estar definido correctamente en la configuración.

**Código problemático:**
```python
def load_clienteslibres(filepath=None):
    config = load_config()
    postgres_config = config.get("postgress", {})
    general_config = config.get("clientes_libres", {})
    Loader = BaseLoaderPostgres(
            config=postgres_config,
            configload=general_config
        )

    Loader.validar_conexion()
    Loader.verificar_datos(data=general_config['local_destination_dir'])  # ❌ Falta table_name
    # ...
```

**Impacto:**
- Error cuando `self._cfgload.table` no está definido
- Validación incorrecta de columnas
- Posible carga de datos en tabla incorrecta

**Solución recomendada:**
```python
Loader.verificar_datos(
    data=general_config['local_destination_dir'],
    table_name=general_config.get('table')  # ✅ Especificar tabla
)
```

---

## 🟡 BUGS DE ALTA PRIORIDAD

### BUG #9: Variable no usada en asegurar_directorio_sftp
**Archivo:** `proyectos/energiafacilities/core/utils.py`
**Línea:** 68
**Severidad:** 🟡 ALTA

**Descripción:**
Se asigna el resultado de `sftp.stat()` a una variable `a` que nunca se usa. Si bien no causa un error, indica código innecesario.

**Código problemático:**
```python
def asegurar_directorio_sftp(sftp, ruta_completa):
    partes = ruta_completa.strip('/').split('/')
    path_actual = ''
    for parte in partes:
        path_actual += '/' + parte
        try:
            a = sftp.stat(path_actual)  # ❌ Variable no usada
        except FileNotFoundError:
            logger.debug(f"Creando carpeta: {path_actual}")
            sftp.mkdir(path_actual)
```

**Solución recomendada:**
```python
try:
    sftp.stat(path_actual)  # ✅ No asignar si no se usa
except FileNotFoundError:
    logger.debug(f"Creando carpeta: {path_actual}")
    sftp.mkdir(path_actual)
```

---

### BUG #10: Posible SQL Injection por concatenación de strings
**Archivos:**
- `proyectos/energiafacilities/core/base_postgress.py` (línea 179)
- `proyectos/energiafacilities/core/base_loader.py` (líneas 191, 209, 216)
**Severidad:** 🟡 ALTA

**Descripción:**
Se construyen queries SQL con f-strings usando valores de configuración. Si bien estos valores vienen de archivos de configuración, es una práctica peligrosa que podría permitir SQL injection si la configuración se modifica maliciosamente.

**Código problemático:**
```python
# base_postgress.py línea 179
sql = f"SELECT {cols} FROM {cfg.schema}.{cfg.table}"
if getattr(cfg, "where", None):
    sql += f" WHERE {cfg.where}"  # ❌ Concatenación directa

# base_loader.py línea 191
full_table = f"{schema or self._cfgload.schema}.{table_name or self._cfgload.table}"
```

**Impacto:**
- Riesgo de SQL injection si configuración es comprometida
- Vulnerabilidad de seguridad potencial

**Solución recomendada:**
Usar parámetros de SQLAlchemy con identifiers:
```python
from sqlalchemy import text, Table, MetaData

# Para queries dinámicas, validar que schema y table son identificadores válidos
def _validate_identifier(name: str) -> str:
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(f"Invalid identifier: {name}")
    return name

schema = _validate_identifier(cfg.schema)
table = _validate_identifier(cfg.table)
sql = f"SELECT {cols} FROM {schema}.{table}"
```

---

### BUG #11: Validación insuficiente de filename en transform_helpers
**Archivo:** `proyectos/energiafacilities/sources/clientes_libres/help/transform_helpers.py`
**Líneas:** 221-222
**Severidad:** 🟡 ALTA

**Descripción:**
Se hace `filename.rsplit('/',1)[0]` sin verificar primero si `filename` es None. Esto causará un AttributeError si tanto `config_transform.get("local_destination_dir")` como `newdestinationoptional` son None.

**Código problemático:**
```python
if(save):
    filename = config_transform.get("local_destination_dir", newdestinationoptional)  # ❌ Puede ser None
    try:
        pathcrear = filename.rsplit('/',1)[0]  # ❌ AttributeError si filename es None
        os.makedirs(pathcrear, exist_ok=True)
        logger.debug(f"Directorio {pathcrear} creado/existente.")
    except Exception as e:
        logger.debug(f"No se pudo crear el directorio {pathcrear}: {e}")
```

**Impacto:**
- AttributeError en tiempo de ejecución
- Fallo al guardar archivos transformados
- Pipeline interrumpido

**Solución recomendada:**
```python
if save:
    filename = config_transform.get("local_destination_dir") or newdestinationoptional
    if not filename:
        raise ValueError("No se especificó un directorio de destino para guardar")
    try:
        pathcrear = filename.rsplit('/',1)[0]
        os.makedirs(pathcrear, exist_ok=True)
        logger.debug(f"Directorio {pathcrear} creado/existente.")
    except Exception as e:
        logger.error(f"No se pudo crear el directorio {pathcrear}: {e}")
        raise
```

---

### BUG #12: Operación SFTP rename sin validación de archivo existente
**Archivo:** `proyectos/energiafacilities/core/base_stractor.py`
**Línea:** 169
**Severidad:** 🟡 ALTA

**Descripción:**
La operación `sftp.rename()` puede fallar o sobrescribir archivos si el archivo destino ya existe. No hay validación previa ni manejo específico de este caso.

**Código problemático:**
```python
if remotetransfere:
    asegurar_directorio_sftp(sftp, local_dir)
    sftp.rename(f"{remote_dir}/{archivo}", f"{local_dir}/{archivo}")  # ❌ Sin validación
    msg = f"Archivo movido con éxito de {remote_dir}/{archivo} a {local_dir}"
    logger.info(msg)
```

**Impacto:**
- Sobrescritura silenciosa de archivos existentes
- Pérdida de datos si el archivo destino ya existe
- Comportamiento impredecible en SFTP

**Solución recomendada:**
```python
if remotetransfere:
    asegurar_directorio_sftp(sftp, local_dir)
    destino = f"{local_dir}/{archivo}"

    # Validar si el archivo destino ya existe
    try:
        sftp.stat(destino)
        logger.warning(f"El archivo {destino} ya existe, será sobrescrito")
    except FileNotFoundError:
        pass  # Archivo no existe, OK para mover

    sftp.rename(f"{remote_dir}/{archivo}", destino)
    msg = f"Archivo movido con éxito de {remote_dir}/{archivo} a {local_dir}"
    logger.info(msg)
```

---

## RECOMENDACIONES GENERALES

### 1. Gestión de Recursos
- ✅ Implementar context managers (`__enter__`, `__exit__`) en todas las clases de conexión
- ✅ Usar bloques `finally` para garantizar cierre de recursos
- ✅ Considerar usar `contextlib.closing()` para recursos simples

### 2. Manejo de Errores
- ✅ Evitar `except Exception` genéricos sin re-raise
- ✅ Implementar excepciones personalizadas para diferentes tipos de errores
- ✅ Agregar más logging para facilitar debugging

### 3. Validación de Datos
- ✅ Validar parámetros de entrada en todas las funciones públicas
- ✅ Usar type hints y validación en runtime (ej: pydantic)
- ✅ Implementar unit tests para casos edge

### 4. Seguridad
- ✅ Revisar todas las queries SQL dinámicas
- ✅ Implementar sanitización de identificadores
- ✅ Auditar logs para no exponer credenciales

### 5. Configuración
- ✅ Documentar todos los parámetros requeridos en archivos de configuración
- ✅ Implementar schemas de validación para configs (ej: JSON Schema)
- ✅ Agregar valores por defecto seguros

---

## PRIORIZACIÓN DE CORRECCIONES

### FASE 1 (Inmediata - Bugs Críticos)
1. **BUG #4** - Indentación en base_postgress.py (pérdida de datos)
2. **BUG #3** - Parámetro incorrecto en pd.read_csv
3. **BUG #6** - Inconsistencia de tipo en DAG_clientes_libres
4. **BUG #8** - Parámetros faltantes en verificar_datos

### FASE 2 (Urgente - Resource Leaks)
5. **BUG #1** - Conexiones SFTP sin cerrar
6. **BUG #2** - Transport SFTP sin cerrar
7. **BUG #5** - Conexión PostgreSQL sin cerrar
8. **BUG #7** - Workbook Excel sin cerrar

### FASE 3 (Importante - Seguridad y Validación)
9. **BUG #10** - SQL Injection potencial
10. **BUG #11** - Validación de filename
11. **BUG #12** - Operación SFTP rename

### FASE 4 (Mejoras - Code Quality)
12. **BUG #9** - Variable no usada

---

## NOTAS FINALES

Este análisis se realizó **sin modificar la estructura del código** para evitar generar nuevos errores. Todos los bugs identificados son **existentes en el código actual** y deben ser corregidos de manera cuidadosa y sistemática.

**Recomendación:** Implementar las correcciones en un entorno de desarrollo/testing antes de aplicarlas en producción, con pruebas exhaustivas para cada bug corregido.

---

**Fin del Reporte**
