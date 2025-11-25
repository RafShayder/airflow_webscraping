# EnergiaFacilities

Framework para automatización de scraping y ETL desde diferentes fuentes de datos de Integratel/Teleows.

## Estructura del Proyecto

```
energiafacilities/
├── clients/          # Clientes de alto nivel para Selenium
├── common/           # Utilidades compartidas
├── config/           # Configuraciones YAML y mapeos de columnas
├── core/             # Funciones base y utilidades del framework
└── sources/          # Módulos de extracción por fuente de datos
    ├── autin_gde/
    ├── autin_checklist/
    ├── base_sitios/
    ├── cargaglobal/
    ├── clientes_libres/
    ├── sftp_energia/
    ├── sftp_pago_energia/
    ├── sftp_toa/
    └── webindra/
```

## Directorios Principales

### `clients/`
Clientes de alto nivel que encapsulan interacciones con Selenium:
- **AuthManager**: Login y gestión de sesión en portal Integratel
- **BrowserManager**: Configuración de Chrome/Chromium con rutas de descarga
- **FilterManager**: Utilidades para abrir paneles de filtro
- **IframeManager**: Cambios de contexto entre iframes
- **DateFilterManager**: Gestión de filtros de fecha (unificado para GDE y Checklist)
- **LogManagementManager**: Navegación y descarga desde módulo Log Management

### `common/`
Utilidades compartidas entre módulos:
- **dynamic_checklist_constants.py**: Constantes para Dynamic Checklist
- **selenium_utils.py**: Helpers para Selenium
- **vue_helpers.py**: Utilidades para interactuar con componentes Vue.js

### `config/`
Configuraciones y mapeos:
- **config_*.yaml**: Configuraciones por entorno (dev, prod, staging)
- **columnas/**: Mapeos de columnas y transformaciones

### `core/`
Funciones base del framework:
- **utils.py**: Carga de configuración, logging
- **airflow_utils.py**: Utilidades para integración con Airflow
- **base_*.py**: Clases base para loaders, transformers, exporters

### `sources/`
Módulos de extracción por fuente de datos. Cada módulo típicamente contiene:
- **stractor.py**: Workflow de scraping/extracción (Selenium o SFTP)
- **loader.py**: Carga de datos a PostgreSQL
- **transformer.py**: Transformaciones de datos (si aplica)
- **run_sp.py**: Ejecución de stored procedures (si aplica)
- **geterrortable.py**: Manejo de errores y tablas de error (si aplica)

## Módulos de Extracción

### `autin_gde/`
**Proceso**: Extracción del reporte Console GDE Export desde Integratel.

**Flujo**:
1. Login al portal Integratel
2. Navegación a módulo GDE
3. Aplicación de filtros (Task Type: CM/OPM, fechas)
4. Exportación del reporte
5. Monitoreo de estado de exportación
6. Descarga del archivo Excel

**Archivos**:
- `stractor.py`: Workflow completo de scraping
- `loader.py`: Carga datos desde Excel a PostgreSQL (tabla `raw.web_mm_autin_infogeneral`)

---

### `autin_checklist/`
**Proceso**: Extracción del reporte Dynamic Checklist > Sub PM Query desde Integratel.

**Flujo**:
1. Login al portal Integratel
2. Navegación a Dynamic Checklist > Sub PM Query
3. Aplicación de filtros de fecha
4. Exportación del reporte
5. Descarga directa o vía Log Management
6. Carga de múltiples tablas desde Excel

**Archivos**:
- `stractor.py`: Workflow completo de scraping
- `loader.py`: Carga datos desde múltiples pestañas del Excel a PostgreSQL (11 tablas en schema `raw`)

---

### `base_sitios/`
**Proceso**: Extracción de base de sitios desde SFTP.

**Flujo**:
1. Conexión SFTP
2. Descarga de archivo
3. Carga a PostgreSQL

**Archivos**:
- `stractor.py`: Descarga desde SFTP
- `loader.py`: Carga a tabla `raw.FS_MM_BASE_DE_SITIOS`

---

### `clientes_libres/`
**Proceso**: Procesamiento de recibos manuales desde SFTP.

**Flujo**:
1. Conexión SFTP
2. Descarga de archivo Excel
3. Transformación de datos
4. Carga a PostgreSQL

**Archivos**:
- `stractor.py`: Descarga desde SFTP
- `transformer.py`: Transformaciones de datos
- `loader.py`: Carga a tabla `raw.sftp_mm_clientes_libres`

---

### `sftp_energia/`
**Proceso**: Extracción de consumos de energía desde SFTP.

**Flujo**:
1. Conexión SFTP
2. Descarga de archivos (PD y DA)
3. Carga a PostgreSQL
4. Ejecución de stored procedures
5. Manejo de errores

**Archivos**:
- `stractor.py`: Descarga desde SFTP
- `loader.py`: Carga a tablas `raw.SFTP_MM_CONSUMO_SUMINISTRO_PD` y `raw.SFTP_MM_CONSUMO_SUMINISTRO_DA`
- `run_sp.py`: Ejecución de stored procedures de transformación
- `geterrortable.py`: Manejo de errores y tablas de error

---

### `sftp_pago_energia/`
**Proceso**: Extracción de pagos de energía desde SFTP.

**Flujo**:
1. Conexión SFTP (usa misma conexión que `sftp_energia`)
2. Descarga y consolidación de archivos Excel
3. Validación de archivos
4. Carga a PostgreSQL
5. Ejecución de stored procedures
6. Manejo de errores

**Archivos**:
- `stractor.py`: Descarga y consolidación desde SFTP
- `loader.py`: Carga a tabla `raw.sftp_mm_pago_energia`
- `run_sp.py`: Ejecución de stored procedures de transformación
- `geterrortable.py`: Manejo de errores y tablas de error

---

### `sftp_toa/`
**Proceso**: Extracción de reportes TOA desde SFTP.

**Flujo**:
1. Conexión SFTP
2. Descarga de archivo Excel
3. Carga a PostgreSQL
4. Ejecución de stored procedures

**Archivos**:
- `stractor.py`: Descarga desde SFTP
- `loader.py`: Carga a tabla `raw.sftp_hd_toa`
- `run_sp.py`: Ejecución de stored procedures de transformación

---

### `webindra/`
**Proceso**: Extracción de recibos de energía desde portal web Indra.

**Flujo**:
1. Login al portal web
2. Navegación y exportación de recibos
3. Descarga de archivo Excel
4. Carga a PostgreSQL
5. Ejecución de stored procedures
6. Manejo de errores

**Archivos**:
- `stractor.py`: Scraping del portal web
- `loader.py`: Carga a tabla `raw.web_mm_indra_energia`
- `run_sp.py`: Ejecución de stored procedures de transformación
- `geterrortable.py`: Manejo de errores y tablas de error

---

### `cargaglobal/`
**Proceso**: Carga manual de datos.

**Archivos**:
- `cargamanual.py`: Script para carga manual

---

## Uso

### Desde código Python

```python
from energiafacilities.core.utils import load_config, setup_logging

# Configurar logging
setup_logging("INFO")

# Cargar configuración (detecta automáticamente el entorno desde ENV_MODE)
config = load_config()

# Obtener configuración de un módulo específico
postgres_config = config.get("postgress", {})
sftp_config = config.get("sftp_energia_c", {})
gde_config = config.get("gde", {})
```

### Desde Airflow DAGs

Los DAGs utilizan automáticamente las Connections y Variables de Airflow configuradas. Ver documentación en `AIRFLOW_SETUP_{ENV}.md` para detalles completos.

## Configuración

El sistema de configuración utiliza múltiples fuentes con la siguiente **prioridad**:

1. **Airflow Variables** (mayor prioridad)
2. **Airflow Connections**
3. **YAML** (`config/config_{env}.yaml`)
4. **Variables de entorno** (usadas para reemplazo en YAML con `${VAR_NAME}`)

### Archivos YAML

Las configuraciones base se encuentran en `config/config_*.yaml`:
- **config_dev.yaml**: Desarrollo
- **config_prod.yaml**: Producción
- **config_staging.yaml**: Staging

Cada sección del YAML corresponde a un módulo de extracción y contiene:
- Credenciales de conexión
- Rutas de archivos (remotos y locales)
- Filtros y parámetros específicos
- Configuración de base de datos (tablas, schemas, stored procedures)
- Configuración de manejo de errores

### Airflow Connections

Las Connections almacenan credenciales y configuración de conexión. El sistema busca automáticamente connections con el formato `{conn_id}_{env}` (ej: `postgres_siom_dev`, `sftp_energia_dev`).

**Ejemplo de Connection para PostgreSQL:**
```
Connection Id: postgres_siom_dev
Connection Type: postgres
Host: 10.226.17.162
Port: 5425
Schema: siom_dev
Login: siom_dev
Password: s10m#d3v
Extra (JSON): {"application_name":"airflow","sslmode":"prefer"}
```

### Airflow Variables

Las Variables permiten sobrescribir valores específicos del YAML. Se buscan con prefijo basado en el nombre de la sección:

**Ejemplo de Variables:**
```
ENV_MODE = "dev"
LOGGING_LEVEL = "INFO"
POSTGRES_HOST_DEV = "10.226.17.162"
SFTP_ENERGIA_HOST_DEV = "10.252.206.132"
TELEOWS_GDE_USERNAME_DEV = "Integratel_Data"
```

### Secciones Mapeadas

Las siguientes secciones tienen mapeo explícito en `core/utils.py`:
- `postgress` → Connection: `postgres_siom_{env}`, Variables: `POSTGRES_*`
- `gde` → Connection: `generic_autin_gde_{env}`, Variables: `TELEOWS_GDE_*`
- `dynamic_checklist` → Connection: `generic_autin_dc_{env}`, Variables: `TELEOWS_DC_*`
- `sftp_energia_c` → Connection: `sftp_energia_{env}`, Variables: `SFTP_ENERGIA_*`
- `sftp_base_sitios` → Connection: `sftp_base_sitios_{env}`, Variables: `SFTP_BASE_SITIOS_*`
- `webindra_energia` → Connection: `http_webindra_{env}`, Variables: `WEBINDRA_ENERGIA_*`
- Y otras más...

Las secciones no mapeadas usan **auto-descubrimiento** basado en convenciones.

### Variables Requeridas

**Variables globales que deben configurarse en Airflow:**
- `ENV_MODE`: Define el entorno actual (`dev`, `staging`, o `prod`). **REQUERIDA**.
- `LOGGING_LEVEL`: Nivel de logging (`INFO`, `DEBUG`, `WARNING`, `ERROR`, `CRITICAL`). **REQUERIDA**.

Para una guía completa de todas las Connections y Variables necesarias, consulta los archivos `AIRFLOW_SETUP_{ENV}.md` en la raíz del proyecto (no versionados).

