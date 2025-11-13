# Dependencias de autin_checklist

Este documento describe todas las dependencias utilizadas en el módulo `autin_checklist`, su origen y propósito.

## Biblioteca estándar de Python

- **`logging`**: Sistema de logging para mensajes informativos/debug/error
- **`os`**: Variables de entorno (PROXY, ENV_MODE) y detección de paths del sistema
- **`sys`**: Configuración de sys.path para imports cuando se ejecuta directamente
- **`time`**: Timestamps para monitoreo de exportación (time.time())
- **`dataclasses.dataclass`**: Decorador para DynamicChecklistConfig (configuración estructurada)
- **`pathlib.Path`**: Manipulación de rutas de archivos y directorios
- **`time.sleep`**: Delays entre acciones de UI para estabilidad
- **`typing`**: Type hints (Any, Dict, Iterable, Optional) para mejor mantenibilidad

## Selenium (dependencia externa)

- **`selenium.common.exceptions.TimeoutException`**: Manejo de timeouts en esperas
- **`selenium.webdriver.common.by.By`**: Localizadores de elementos (XPATH, CSS_SELECTOR)
- **`selenium.webdriver.support.expected_conditions.EC`**: Condiciones de espera (element_to_be_clickable, etc.)
- **`selenium.webdriver.support.ui.WebDriverWait`**: Esperas explícitas con timeouts

## energiafacilities.clients (módulo interno)

- **`AuthManager`**: Login al portal Integratel
  - Ubicación: `clients/auth.py`
  - Propósito: Autenticación en el portal Integratel/Teleows

- **`BrowserManager`**: Configuración de Chrome/Chromium con proxy, headless, rutas de descarga
  - Ubicación: `clients/browser.py`
  - Propósito: Gestión del navegador Selenium con configuración de proxy y rutas de descarga

- **`FilterManager`**: Apertura del panel de filtros con método "simple"
  - Ubicación: `clients/filters.py`
  - Propósito: Abrir el panel de filtros usando método simplificado

- **`IframeManager`**: Cambio de contexto entre iframes del portal
  - Ubicación: `clients/iframes.py`
  - Propósito: Cambiar entre diferentes iframes del portal Integratel

- **`DateFilterManager`**: Aplicación de filtros de fecha usando Create Time o radio buttons
  - Ubicación: `clients/date_filter_manager.py`
  - Propósito: Gestión unificada de filtros de fecha (compartido con GDE)

- **`LogManagementManager`**: Navegación al módulo Log Management, refresco de tabla y descarga
  - Ubicación: `clients/log_management_manager.py`
  - Propósito: Navegar al módulo Log Management, refrescar la tabla de exportaciones y descargar archivos cuando la exportación no es directa

## energiafacilities.common (módulo interno)

- **`click_with_retry`**: Helper para clicks robustos con reintentos automáticos
  - Ubicación: `common/`
  - Propósito: Realizar clicks con reintentos automáticos en caso de fallo

- **`monitor_export_loader`**: Detecta si la exportación va por descarga directa o Log Management
  - Ubicación: `common/`
  - Propósito: Monitorear el proceso de exportación y determinar si hay descarga directa o si debe irse a Log Management

- **`navigate_to_menu_item`**: Navegación al menú lateral del portal
  - Ubicación: `common/`
  - Propósito: Navegar a elementos del menú lateral (ej: Dynamic Checklist)

- **`navigate_to_submenu`**: Navegación a submenús dentro de un menú principal
  - Ubicación: `common/`
  - Propósito: Navegar a submenús dentro de un menú principal

- **`require`**: Helper para validar condiciones y lanzar excepciones descriptivas
  - Ubicación: `common/`
  - Propósito: Validación de condiciones con mensajes de error claros

- **`wait_for_download`**: Detección y renombrado de archivos descargados con manejo de conflictos
  - Ubicación: `common/`
  - Propósito: Esperar y gestionar archivos descargados, incluyendo resolución de conflictos de nombres

- **`wait_for_notification_to_clear`**: Espera a que las notificaciones del portal desaparezcan
  - Ubicación: `common/`
  - Propósito: Esperar a que las notificaciones del portal se oculten antes de continuar

## energiafacilities.common.dynamic_checklist_constants (módulo interno)

- **Constantes específicas de Dynamic Checklist**
  - Ubicación: `common/dynamic_checklist_constants.py`
  - Propósito: Centralizar todas las constantes específicas del módulo
  - Incluye:
    - Índices de menú (`MENU_INDEX_DYNAMIC_CHECKLIST`)
    - Timeouts (`SPLITBUTTON_TIMEOUT`, `LOADER_TIMEOUT`, `DOWNLOAD_TIMEOUT`)
    - Selectores CSS/XPATH (`CSS_SPLITBUTTON_TEXT`, `XPATH_SPLITBUTTON_BY_TEXT`, etc.)
    - Textos de botones (`BUTTON_FILTER`, `BUTTON_EXPORT`)
    - Nombres de menús (`MENU_DYNAMIC_CHECKLIST`, `SUBMENU_SUB_PM_QUERY`)

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

- **`dynamic_checklist`**: Configuración específica de Dynamic Checklist
  - Fechas (date_mode, date_from, date_to, last_n_days)
  - Nombre de archivo de salida (specific_filename)
  - Tabla destino en PostgreSQL
  - Rutas locales (local_dir)
  - Modo de carga (load_mode: append/replace)

### Archivos JSON

- **`config/columnas/columns_map_checklist.json`**: Mapeo de columnas Excel → PostgreSQL
  - 11 tablas: `cf_banco_de_baterias`, `cf_bastidor_distribucion`, `cf_cuadro_de_fuerza`, `cf_modulos_rectificadores`, `cf_tablero_ac_de_cuadro_de_fu`, `cf_descarga_controlada_bater`, `ie_datos_spat_general`, `ie_mantenimiento_pozo_por_poz`, `ie_suministro_de_energia`, `ie_tablero_principal`, `ie_tablero_secundario`
  - Propósito: Mapear nombres de columnas del Excel exportado a nombres de columnas en la base de datos para cada una de las 11 pestañas

## Flujo de dependencias

1. **Configuración**: `load_config()` carga desde YAML → `DynamicChecklistConfig`
2. **Navegador**: `BrowserManager` crea driver Selenium con configuración
3. **Autenticación**: `AuthManager` realiza login
4. **Navegación**: `navigate_to_menu_item()` y `navigate_to_submenu()` navegan a Dynamic Checklist > Sub PM Query
5. **Contexto**: `IframeManager` gestiona cambios de iframe
6. **Filtros**: `FilterManager` abre panel de filtros, `DateFilterManager` aplica filtros de fecha
7. **Exportación**: `_click_splitbutton()` lanza exportación, `monitor_export_loader()` detecta tipo de descarga
8. **Descarga directa**: `wait_for_download` detecta y renombra archivo
9. **Descarga vía Log Management**: `LogManagementManager` navega al módulo, refresca tabla y descarga
10. **Carga**: `loader.py` procesa 11 pestañas usando `columns_map_checklist.json` para mapear columnas

