# Dependencias de autin_gde

Este documento describe todas las dependencias utilizadas en el módulo `autin_gde`, su origen y propósito.

## Biblioteca estándar de Python

- **`logging`**: Sistema de logging para mensajes informativos/debug/error
- **`os`**: Variables de entorno (PROXY, ENV_MODE) y detección de paths del sistema
- **`sys`**: Configuración de sys.path para imports cuando se ejecuta directamente
- **`time`**: Timestamps para monitoreo de exportación (time.time())
- **`dataclasses.dataclass`**: Decorador para GDEConfig (configuración estructurada)
- **`datetime.datetime, timedelta`**: Cálculo de rangos de fechas (last_n_days)
- **`pathlib.Path`**: Manipulación de rutas de archivos y directorios
- **`time.sleep`**: Delays entre acciones de UI para estabilidad
- **`typing`**: Type hints (Any, Dict, Iterable, List, Optional) para mejor mantenibilidad

## Selenium (dependencia externa)

- **`selenium.webdriver.common.by.By`**: Localizadores de elementos (XPATH, CSS_SELECTOR)
- **`selenium.webdriver.support.expected_conditions.EC`**: Condiciones de espera (element_to_be_clickable, etc.)
- **`selenium.webdriver.support.ui.WebDriverWait`**: Esperas explícitas con timeouts
- **`selenium.common.exceptions.ElementClickInterceptedException, TimeoutException`**: Manejo de excepciones específicas de Selenium
- **`selenium.webdriver.common.keys.Keys`**: Simulación de teclas (ENTER, CONTROL+A, DELETE)

## energiafacilities.clients (módulo interno)

- **`AuthManager`**: Login al portal Integratel
  - Ubicación: `clients/auth.py`
  - Propósito: Autenticación en el portal Integratel/Teleows

- **`BrowserManager`**: Configuración de Chrome/Chromium con proxy, headless, rutas de descarga
  - Ubicación: `clients/browser.py`
  - Propósito: Gestión del navegador Selenium con configuración de proxy y rutas de descarga

- **`DateFilterManager`**: Aplicación de filtros de fecha usando Create Time o radio buttons
  - Ubicación: `clients/date_filter_manager.py`
  - Propósito: Gestión unificada de filtros de fecha (compartido con Dynamic Checklist)

- **`FilterManager`**: Apertura y gestión del panel de filtros
  - Ubicación: `clients/filters.py`
  - Propósito: Abrir y gestionar el panel de filtros del portal

- **`IframeManager`**: Cambio de contexto entre iframes del portal
  - Ubicación: `clients/iframes.py`
  - Propósito: Cambiar entre diferentes iframes del portal Integratel

## energiafacilities.common (módulo interno)

- **`require`**: Helper para validar condiciones y lanzar excepciones descriptivas
  - Ubicación: `common/`
  - Propósito: Validación de condiciones con mensajes de error claros

- **`wait_for_download`**: Detección y renombrado de archivos descargados con manejo de conflictos
  - Ubicación: `common/`
  - Propósito: Esperar y gestionar archivos descargados, incluyendo resolución de conflictos de nombres

## energiafacilities.core.utils (módulo interno)

- **`load_config`**: Carga configuración desde YAML con soporte para variables de entorno
  - Ubicación: `core/utils.py`
  - Propósito: Cargar configuración desde archivos YAML (config/config_*.yaml) con interpolación de variables de entorno

- **`default_download_path`**: Retorna path de descarga por defecto según entorno
  - Ubicación: `core/utils.py`
  - Propósito: Determinar el path de descarga según el entorno (Airflow vs local)

## Configuración

### Archivos YAML

- **`config/config_dev.yaml`**: Configuración para entorno de desarrollo
- **`config/config_prod.yaml`**: Configuración para entorno de producción
- **`config/config_staging.yaml`**: Configuración para entorno de staging

### Secciones de configuración

- **`teleows`**: Configuración general compartida
  - Credenciales (username, password)
  - Proxy
  - Timeouts y configuraciones generales

- **`gde`**: Configuración específica de GDE
  - Fechas (date_mode, date_from, date_to, last_n_days)
  - Nombre de archivo de salida (gde_output_filename)
  - Tabla destino en PostgreSQL
  - Rutas locales (local_dir, specific_filename)

### Archivos JSON

- **`config/columnas/columns_map_gde.json`**: Mapeo de columnas Excel → PostgreSQL
  - Tabla: `gde_tasks`
  - Propósito: Mapear nombres de columnas del Excel exportado a nombres de columnas en la base de datos

## Flujo de dependencias

1. **Configuración**: `load_config()` carga desde YAML → `GDEConfig`
2. **Navegador**: `BrowserManager` crea driver Selenium con configuración
3. **Autenticación**: `AuthManager` realiza login
4. **Navegación**: `IframeManager` y `FilterManager` gestionan contexto y filtros
5. **Filtros de fecha**: `DateFilterManager` aplica filtros según configuración
6. **Descarga**: `wait_for_download` detecta y renombra archivo descargado
7. **Carga**: `loader.py` usa `columns_map_gde.json` para mapear columnas

