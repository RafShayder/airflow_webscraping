# Integración de Teleows con Airflow

Esta guía explica cómo usar el sistema ETL de GDE en Apache Airflow, incluyendo la configuración de credenciales.

---

## 📋 Índice

1. [Conceptos Clave](#conceptos-clave)
2. [Instalación del SP en PostgreSQL](#instalación-del-sp-en-postgresql)
3. [Configuración de Credenciales](#configuración-de-credenciales)
4. [Ejemplo de DAG](#ejemplo-de-dag)
5. [Preguntas Frecuentes](#preguntas-frecuentes)

---

## 🎯 Conceptos Clave

### ¿Qué es el Stored Procedure (SP)?

El **Stored Procedure** es un programa SQL que vive **DENTRO de PostgreSQL** y transforma los datos:

```
📦 PostgreSQL Database
  ├── 📁 raw.gde_tasks (datos crudos)
  ├── 📁 ods.gde_tasks (datos procesados)
  └── ⚙️  ods.sp_cargar_gde_tasks() ← VIVE AQUÍ
```

**NO es un archivo Python** que se ejecuta desde Airflow, sino un **procedimiento en la base de datos**.

### ¿Qué hace `run_sp.py`?

El archivo `run_sp.py` es un **wrapper Python** que:

1. Se conecta a PostgreSQL
2. Ejecuta el comando SQL: `CALL ods.sp_cargar_gde_tasks()`
3. Obtiene los logs de ejecución
4. Retorna el resultado a Python/Airflow

```python
# run_sp.py hace esto internamente:
postgres.ejecutar("ods.sp_cargar_gde_tasks", tipo='sp')
```

---

## 🗄️ Instalación del SP en PostgreSQL

### ⚠️ IMPORTANTE: Esto se hace UNA SOLA VEZ

El archivo SQL (`sp_cargar_gde_tasks.sql`) **NO se ejecuta en cada ejecución del DAG**.

Se ejecuta **una vez** para **instalar** el SP en PostgreSQL.

### Cuándo ejecutar el SQL

| Situación | Acción |
|-----------|--------|
| Primera vez configurando el sistema | ✅ Ejecutar SQL |
| Actualizar la lógica del SP | ✅ Ejecutar SQL |
| Cada vez que corre el DAG | ❌ NO ejecutar |
| Cambió la estructura de tablas | ✅ Ejecutar SQL |

### Cómo instalar (3 opciones)

#### **Opción 1: Manualmente desde terminal**

```bash
# Desde tu máquina o servidor con acceso a PostgreSQL
psql -h tu-postgres-host \
     -U tu-usuario \
     -d tu-base-datos \
     -f proyectos/teleows/sql/sp_cargar_gde_tasks.sql
```

#### **Opción 2: Desde DBeaver/pgAdmin**

1. Conectarse a PostgreSQL
2. Abrir `sp_cargar_gde_tasks.sql`
3. Ejecutar todo el script (F5)
4. Verificar que no haya errores

#### **Opción 3: Desde un DAG de Airflow (una sola vez)**

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG QUE SOLO SE EJECUTA UNA VEZ PARA INSTALAR
with DAG(
    'install_gde_sp',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Manual
    catchup=False,
    tags=['setup', 'one-time']
) as dag:

    install_sp = BashOperator(
        task_id='install_stored_procedure',
        bash_command="""
        psql -h {{ var.value.POSTGRES_HOST }} \
             -U {{ var.value.POSTGRES_USER }} \
             -d {{ var.value.POSTGRES_DB }} \
             -f /opt/airflow/proyectos/teleows/sql/sp_cargar_gde_tasks.sql
        """,
        env={
            'PGPASSWORD': '{{ var.value.POSTGRES_PASS }}'
        }
    )
```

### Verificar que el SP está instalado

```sql
-- Conectarse a PostgreSQL y ejecutar:
SELECT n.nspname as schema, p.proname as procedure_name
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE p.proname = 'sp_cargar_gde_tasks'
  AND n.nspname = 'ods';

-- Debería retornar:
-- schema | procedure_name
-- ods    | sp_cargar_gde_tasks
```

---

## 🔐 Configuración de Credenciales

Hay **2 formas** de configurar credenciales en Airflow:

### Opción 1: Variables de Airflow (Recomendada)

#### Ventajas
✅ Centralizadas en Airflow UI
✅ Fácil de cambiar sin tocar código
✅ Puede ser diferentes por entorno (dev/prod)
✅ Se pueden encriptar

#### Configurar Variables en Airflow UI

1. **Ir a Admin → Variables**
2. **Agregar las siguientes variables**:

| Key | Value | Descripción |
|-----|-------|-------------|
| `POSTGRES_HOST` | `tu-host.amazonaws.com` | Host de PostgreSQL |
| `POSTGRES_PORT` | `5432` | Puerto |
| `POSTGRES_DB` | `teleows_db` | Nombre de BD |
| `POSTGRES_USER` | `etl_user` | Usuario |
| `POSTGRES_PASS` | `***********` | Contraseña (marcada como sensible) |
| `USERNAME` | `tu_usuario_teleows` | Usuario web scraping |
| `PASSWORD` | `***********` | Password web scraping |

3. **Marcar como sensible** (checkbox) las contraseñas

#### Usar en el código

```python
from airflow.models import Variable

# Leer variables de Airflow
postgres_config = {
    "host": Variable.get("POSTGRES_HOST"),
    "port": int(Variable.get("POSTGRES_PORT", "5432")),
    "database": Variable.get("POSTGRES_DB"),
    "user": Variable.get("POSTGRES_USER"),
    "password": Variable.get("POSTGRES_PASS")
}

# Pasar al sistema ETL
from teleows.config import load_yaml_config

# El sistema cargará automáticamente desde variables de entorno
# que Airflow puede setear
```

---

### Opción 2: Connections de Airflow (Más avanzada)

#### Ventajas
✅ Estandarizado para PostgreSQL
✅ Integración nativa con operadores de Airflow
✅ Manejo automático de conexiones

#### Configurar Connection en Airflow UI

1. **Ir a Admin → Connections**
2. **Agregar nueva conexión**:

| Campo | Valor |
|-------|-------|
| Connection Id | `postgres_teleows` |
| Connection Type | `Postgres` |
| Host | `tu-host.amazonaws.com` |
| Schema | `teleows_db` |
| Login | `etl_user` |
| Password | `********` |
| Port | `5432` |

3. **Test** la conexión

#### Usar en el código

```python
from airflow.hooks.postgres_hook import PostgresHook

# Opción A: Usar PostgresHook directamente
def ejecutar_sp_con_hook():
    hook = PostgresHook(postgres_conn_id='postgres_teleows')

    # Ejecutar SP
    hook.run("CALL ods.sp_cargar_gde_tasks();")

    # Obtener logs
    result = hook.get_records(
        "SELECT * FROM public.log_sp_ultimo_fn('ods.sp_cargar_gde_tasks()')"
    )
    return result

# Opción B: Convertir Connection a dict para teleows
def get_postgres_config_from_connection():
    hook = PostgresHook(postgres_conn_id='postgres_teleows')
    conn = hook.get_connection('postgres_teleows')

    return {
        "host": conn.host,
        "port": conn.port,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password
    }
```

---

### Opción 3: Hybrid (Variables + Connections)

**Recomendación**: Usar **Connections para PostgreSQL** y **Variables para credenciales de web scraping**

```python
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

# PostgreSQL desde Connection
postgres_hook = PostgresHook(postgres_conn_id='postgres_teleows')

# Web scraping desde Variables
scraping_config = {
    "username": Variable.get("TELEOWS_USERNAME"),
    "password": Variable.get("TELEOWS_PASSWORD")
}
```

---

## 📊 Ejemplo de DAG Completo

```python
"""
DAG: GDE ETL Pipeline
Descripción: Extrae datos de GDE, los carga a PostgreSQL y ejecuta transformaciones
Schedule: Diario a las 2 AM
"""

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

# Configuración del DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alerts@empresa.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gde_etl_pipeline',
    default_args=default_args,
    description='ETL completo para GDE (Extract → Load → Transform)',
    schedule_interval='0 2 * * *',  # Diario a las 2 AM
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['gde', 'etl', 'teleows'],
) as dag:

    @task
    def extract_gde():
        """
        PASO 1: Extract - Web scraping de GDE
        """
        import os
        from teleows.sources.gde.stractor import extraer_gde
        from teleows.core.utils import setup_logging

        setup_logging("INFO")

        # Setear variables de entorno desde Airflow Variables
        os.environ['USERNAME'] = Variable.get("TELEOWS_USERNAME")
        os.environ['PASSWORD'] = Variable.get("TELEOWS_PASSWORD")
        os.environ['DOWNLOAD_PATH'] = Variable.get("DOWNLOAD_PATH", "/opt/airflow/tmp")

        # Ejecutar extracción
        filepath = extraer_gde()

        return filepath

    @task
    def load_to_raw(filepath: str):
        """
        PASO 2: Load - Cargar Excel a PostgreSQL RAW
        """
        import os
        from teleows.sources.gde.loader import load_gde
        from airflow.hooks.postgres_hook import PostgresHook

        # Opción A: Usar Connection de Airflow
        hook = PostgresHook(postgres_conn_id='postgres_teleows')
        conn = hook.get_connection('postgres_teleows')

        # Setear variables de entorno para que teleows las use
        os.environ['POSTGRES_HOST'] = conn.host
        os.environ['POSTGRES_PORT'] = str(conn.port)
        os.environ['POSTGRES_DB'] = conn.schema
        os.environ['POSTGRES_USER'] = conn.login
        os.environ['POSTGRES_PASS'] = conn.password

        # Ejecutar carga
        resultado = load_gde(filepath)

        return resultado

    @task
    def transform_with_sp(load_result: dict):
        """
        PASO 3: Transform - Ejecutar Stored Procedure (RAW → ODS)
        """
        from airflow.hooks.postgres_hook import PostgresHook
        import logging

        logger = logging.getLogger(__name__)

        # Conectar a PostgreSQL
        hook = PostgresHook(postgres_conn_id='postgres_teleows')

        # Ejecutar SP
        logger.info("Ejecutando stored procedure: ods.sp_cargar_gde_tasks")
        hook.run("CALL ods.sp_cargar_gde_tasks();")

        # Obtener logs del SP
        logs = hook.get_records(
            "SELECT * FROM public.log_sp_ultimo_fn('ods.sp_cargar_gde_tasks()')"
        )

        if logs:
            log = logs[0]
            logger.info(f"SP ejecutado: {log[1]} - {log[3]} registros procesados")

            if log[1] == 'error':
                raise Exception(f"Error en SP: {log[2]}")

        return {
            "status": "success",
            "registros_procesados": logs[0][3] if logs else 0
        }

    @task
    def validar_datos(transform_result: dict):
        """
        PASO 4: Validación - Verificar calidad de datos
        """
        from airflow.hooks.postgres_hook import PostgresHook
        import logging

        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id='postgres_teleows')

        # Validaciones básicas
        validaciones = {
            "total_registros": hook.get_first(
                "SELECT COUNT(*) FROM ods.gde_tasks"
            )[0],
            "registros_hoy": hook.get_first(
                """
                SELECT COUNT(*) FROM ods.gde_tasks
                WHERE DATE(create_time) = CURRENT_DATE
                """
            )[0],
            "estados_unicos": hook.get_first(
                "SELECT COUNT(DISTINCT task_status) FROM ods.gde_tasks"
            )[0]
        }

        logger.info(f"Validaciones: {validaciones}")

        # Alertar si hay problemas
        if validaciones['total_registros'] == 0:
            raise Exception("⚠️ No hay datos en ods.gde_tasks")

        return validaciones

    # Definir flujo del DAG
    filepath = extract_gde()
    load_result = load_to_raw(filepath)
    transform_result = transform_with_sp(load_result)
    validacion = validar_datos(transform_result)
```

---

## 🔄 Flujo de Ejecución en Airflow

```
┌─────────────────────────────────────────────────────────────┐
│                   AIRFLOW DAG                               │
└─────────────────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
   ┌─────────┐    ┌─────────┐    ┌─────────────┐
   │ Task 1  │    │ Task 2  │    │   Task 3    │
   │ Extract │───▶│  Load   │───▶│ Transform   │
   │         │    │         │    │   (SP)      │
   └─────────┘    └─────────┘    └─────────────┘
        │                │                │
        ▼                ▼                ▼
   Excel en         raw.gde_tasks   ods.gde_tasks
   /tmp/            (PostgreSQL)    (PostgreSQL)
                                         │
                                         ▼
                                  ┌─────────────┐
                                  │   Task 4    │
                                  │ Validación  │
                                  └─────────────┘
```

---

## ❓ Preguntas Frecuentes

### 1. ¿Necesito un archivo `.env` en teleows?

**Respuesta**: Depende del entorno:

| Entorno | Método Recomendado |
|---------|-------------------|
| **Desarrollo Local** | ✅ Archivo `.env` en la raíz |
| **Airflow** | ✅ Variables o Connections de Airflow |
| **Producción** | ✅ Variables de entorno del sistema |

#### Ejemplo de `.env` para desarrollo local:

```bash
# .env en proyectos/teleows/
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=teleows_dev
POSTGRES_USER=postgres
POSTGRES_PASS=mypassword

USERNAME=usuario_web
PASSWORD=pass_web
```

**En Airflow NO necesitas `.env`**, usas Variables/Connections.

---

### 2. ¿Debo ejecutar el SQL en cada ejecución del DAG?

**NO**. El archivo SQL se ejecuta **UNA SOLA VEZ** para instalar el SP.

```python
# ❌ INCORRECTO - No hacer esto en cada ejecución
@task
def ejecutar_sql():
    hook.run(open('sp_cargar_gde_tasks.sql').read())  # ❌ NO

# ✅ CORRECTO - Solo llamar al SP
@task
def ejecutar_sp():
    hook.run("CALL ods.sp_cargar_gde_tasks();")  # ✅ SÍ
```

---

### 3. ¿Puedo usar solo Airflow Connections?

**SÍ**, es la forma más limpia:

```python
from airflow.hooks.postgres_hook import PostgresHook
from teleows.core.base_postgres import PostgresConnector

@task
def ejecutar_etl():
    # Obtener config desde Airflow Connection
    hook = PostgresHook(postgres_conn_id='postgres_teleows')
    conn = hook.get_connection('postgres_teleows')

    # Crear config dict para teleows
    postgres_config = {
        "host": conn.host,
        "port": conn.port,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password
    }

    # Usar con teleows
    pg = PostgresConnector(postgres_config)
    pg.ejecutar("ods.sp_cargar_gde_tasks", tipo='sp')
```

---

### 4. ¿Cómo manejo diferentes ambientes (dev/prod)?

**Opción A: Multiple Connections**

```python
# En dev
postgres_hook = PostgresHook(postgres_conn_id='postgres_teleows_dev')

# En prod
postgres_hook = PostgresHook(postgres_conn_id='postgres_teleows_prod')
```

**Opción B: Variable de entorno**

```python
import os
env = os.getenv('AIRFLOW_ENV', 'dev')
conn_id = f'postgres_teleows_{env}'
postgres_hook = PostgresHook(postgres_conn_id=conn_id)
```

---

### 5. ¿El SP se ejecuta en Airflow o en PostgreSQL?

**En PostgreSQL**. Airflow solo envía el comando:

```python
# Airflow envía:
hook.run("CALL ods.sp_cargar_gde_tasks();")

# PostgreSQL ejecuta:
# - Lee raw.gde_tasks
# - Transforma datos
# - Escribe a ods.gde_tasks
# - Guarda logs

# Airflow recibe:
# - Éxito o error
# - Logs de ejecución
```

---

### 6. ¿Qué pasa si el SP falla?

El SP captura errores y los registra:

```python
@task
def ejecutar_sp_con_manejo_errores():
    hook = PostgresHook(postgres_conn_id='postgres_teleows')

    try:
        # Ejecutar SP
        hook.run("CALL ods.sp_cargar_gde_tasks();")

        # Verificar logs
        logs = hook.get_first(
            "SELECT estado, msj_error FROM public.log_sp_ultimo_fn('ods.sp_cargar_gde_tasks()')"
        )

        if logs[0] == 'error':
            raise Exception(f"SP falló: {logs[1]}")

    except Exception as e:
        # Airflow marcará el task como fallido
        raise
```

---

## 📚 Resumen

| Aspecto | Solución |
|---------|----------|
| **SQL del SP** | Ejecutar UNA VEZ para instalar |
| **Credenciales** | Airflow Variables o Connections |
| **Ejecución del SP** | `hook.run("CALL ods.sp_cargar_gde_tasks()")` |
| **Archivo .env** | Solo para desarrollo local |
| **Logs** | Se guardan en `public.sp_execution_log` |
| **Manejo de errores** | El SP captura y registra errores |

---

## 🎯 Próximos Pasos

1. ✅ Instalar el SP en PostgreSQL (una vez)
2. ✅ Configurar Connections en Airflow
3. ✅ Crear el DAG con el ejemplo de arriba
4. ✅ Probar en desarrollo
5. ✅ Desplegar a producción

