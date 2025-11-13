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
- **stractor.py**: Workflow de scraping (Selenium)
- **loader.py**: Carga de datos a PostgreSQL
- **transformer.py**: Transformaciones de datos (si aplica)

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

**Archivos**:
- `stractor.py`: Descarga desde SFTP
- `loader.py`: Carga a tablas `raw.SFTP_MM_CONSUMO_SUMINISTRO_PD` y `raw.SFTP_MM_CONSUMO_SUMINISTRO_DA`

---

### `webindra/`
**Proceso**: Extracción de recibos de energía desde portal web Indra.

**Flujo**:
1. Login al portal web
2. Navegación y exportación de recibos
3. Descarga de archivo Excel
4. Carga a PostgreSQL

**Archivos**:
- `stractor.py`: Scraping del portal web
- `loader.py`: Carga a tabla `raw.web_mm_indra_energia`

---

### `cargaglobal/`
**Proceso**: Carga manual de datos.

**Archivos**:
- `cargamanual.py`: Script para carga manual

---

## Uso

### Desde código Python

## Configuración

Las configuraciones se encuentran en `config/config_*.yaml`:
- **config_dev.yaml**: Desarrollo
- **config_prod.yaml**: Producción
- **config_staging.yaml**: Staging

Cada sección del YAML corresponde a un módulo de extracción y contiene credenciales, rutas, filtros y parámetros específicos.

