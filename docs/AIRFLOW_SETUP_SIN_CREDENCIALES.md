# Configuracion de Airflow - Guia sin credenciales

Este documento es una guia sin credenciales. Reemplaza `<ENV>` con `dev`, `staging` o `prod` y completa los valores de `Login`/`Password` segun corresponda.

## Variables Globales Requeridas

### ENV_MODE
- **Key**: `ENV_MODE`
- **Value**: `<ENV>`
- **Description**: Define el entorno actual. Esta variable es critica para que el sistema sepa que configuracion usar.

### LOGGING_LEVEL
- **Key**: `LOGGING_LEVEL`
- **Value**: `INFO` (o `DEBUG`, `WARNING`, `ERROR`, `CRITICAL`)
- **Description**: Nivel de logging para el sistema. Valores soportados: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`.

---

## Connections

### 1. PostgreSQL - SIOM <ENV>

**Connection ID**: `postgres_siom_<ENV>`

- **Connection Type**: `Postgres`
- **Host**: `10.226.17.162`
- **Schema**: `siom_<ENV>`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Port**: `5425`
- **Extra** (JSON):
```json
{
  "application_name": "airflow",
  "sslmode": "prefer"
}
```

---

### 2. SFTP Energia

**Connection ID**: `sftp_energia_<ENV>`

- **Connection Type**: `SFTP`
- **Host**: `10.252.206.132`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Port**: `22`
- **Extra** (JSON):
```json
{
  "default_remote_dir": "/REPORTES_CONSUMOS",
  "default_local_dir": "tmp/sftp_energia",
  "schema": "RAW",
  "table_PD": "SFTP_MM_CONSUMO_SUMINISTRO_PD",
  "table_DA": "SFTP_MM_CONSUMO_SUMINISTRO_DA",
  "sp_carga_PD": "ods.sp_cargar_sftp_hm_consumo_suministro_pd",
  "sp_carga_DA": "ods.sp_cargar_sftp_hm_consumo_suministro_da",
  "specific_filename": "reporte-consumo-energia-PD",
  "specific_filename2": "reporte-consumo-energia-DA",
  "if_exists": "replace"
}
```

---

### 2.1 SFTP Pago Energia (dedicada)

**Connection ID**: `sftp_pago_energia_<ENV>`

- **Connection Type**: `SFTP`
- **Host**: `10.252.206.132`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Port**: `22`
- **Extra** (JSON):
```json
{
  "remote_dir": "/PAGOS_ENERGY_ANALYTICS",
  "local_dir": "tmp/sftp_pago_energia",
  "processed_dir": "/PAGOS_ENERGY_ANALYTICS/historico",
  "error_dir": "/PAGOS_ENERGY_ANALYTICS/error",
  "default_sheet": "LIQ. Recibos de Luz",
  "fila_inicial": 6,
  "subsetna": "RECIBO",
  "nombre_salida_local": "Consolidado_PagoEnergia.xlsx",
  "schema": "raw",
  "table": "sftp_mm_pago_energia",
  "if_exists": "replace",
  "sp_carga": "ods.sp_cargar_sftp_hm_pago_energia",
  "errorconfig": {
    "schema": "public",
    "table": "error_pago_energia_ultimo_lote",
    "limmit": 100,
    "remote_dir": "/daas/<ENV>/energy-facilities/errors"
  }
}
```

---

### 3. SFTP DAAS

**Connection ID**: `sftp_daas_<ENV>`

- **Connection Type**: `SFTP`
- **Host**: `10.226.195.181`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Port**: `22`

---

### 4. SFTP Base Sitios

**Connection ID**: `sftp_base_sitios_<ENV>`

- **Connection Type**: `SFTP`
- **Host**: `10.226.195.181`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Port**: `22`
- **Extra** (JSON):
```json
{
  "default_remote_dir": "/daas/<ENV>/energy-facilities/base-sitios/input",
  "default_local_dir": "tmp/sftp_base_sitios",
  "specific_filename": "BASE_SITIOS",
  "schema": "raw",
  "table": "fs_mm_base_de_sitios",
  "sp_carga": "ods.sp_cargar_fs_hm_base_de_sitios",
  "if_exists": "replace"
}
```

---

### 5. SFTP Base Sitios Bitacora

**Connection ID**: `sftp_base_sitios_bitacora_<ENV>`

- **Connection Type**: `SFTP`
- **Host**: `10.226.195.181`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Port**: `22`
- **Extra** (JSON):
```json
{
  "default_remote_dir": "/daas/<ENV>/energy-facilities/base-sitios/input",
  "default_local_dir": "tmp/sftp_base_sitios",
  "specific_filename": "BASE_SITIOS",
  "schema": "RAW",
  "table": "fs_mm_bitacora_base_sitios",
  "sp_carga": "ods.sp_cargar_fs_hm_bitacora_base_sitios",
  "if_exists": "replace"
}
```

---

### 6. SFTP TOA

**Connection ID**: `sftp_toa_<ENV>`

- **Connection Type**: `SFTP`
- **Host**: `10.226.195.181`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Port**: `22`
- **Extra** (JSON):
```json
{
  "default_remote_dir": "/daas/<ENV>/energy-facilities/toa/input",
  "default_local_dir": "tmp/sftp_toa",
  "specific_filename": "TOA",
  "schema": "raw",
  "table": "sftp_hd_toa",
  "if_exists": "replace",
  "sheet_name": 0,
  "page_size": 1000,
  "sp_carga": "ods.sp_cargar_sftp_hd_toa"
}
```

---

### 6.1 SFTP Base Suministros Activos

**Connection ID**: `sftp_base_suministros_activos_<ENV>`

- **Connection Type**: `SFTP`
- **Host**: `10.226.195.181`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Port**: `22`
- **Extra** (JSON):
```json
{
  "remote_dir": "/daas/<ENV>/energy-facilities/base-suministros-activos/input",
  "local_dir": "tmp/sftp_base_suministros_activos",
  "schema": "ods",
  "table": "sftp_hm_base_suministros_activos",
  "if_exists": "replace",
  "specific_filename": "base_suministros_activos",
  "sheet_name": 0,
  "page_size": 1000
}
```

---

### 7. SFTP Clientes Libres

**Connection ID**: `sftp_clientes_libres_<ENV>`

- **Connection Type**: `SFTP`
- **Host**: `10.226.195.181`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Port**: `22`
- **Extra** (JSON):
```json
{
  "default_remote_dir": "/daas/<ENV>/energy-facilities/recibos-manuales/input/",
  "default_local_dir": "tmp/sftp_clientes_libres",
  "processed_destination": "tmp/sftp_clientes_libres/processed/clientes_libres.xlsx",
  "specific_filename": "F-TELEFONICA",
  "schema": "raw",
  "table": "sftp_mm_clientes_libres",
  "sp_carga": "ods.sp_cargar_sftp_hm_clientes_libres",
  "if_exists": "replace"
}
```

---

### 8. HTTP Web Indra

**Connection ID**: `http_webindra_<ENV>`

- **Connection Type**: `HTTP`
- **Host**: `https://recibosttr.com/sistema`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Extra** (JSON):
```json
{
  "headers": {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
  },
  "proxy": "telefonica01.gp.inet:8080",
  "timeout": 120,
  "export_template": "/sisReportes/exportarRecibosSpoutConsumo/{start}/{end}",
  "login_path": "/",
  "period_months": 2,
  "max_retries": 3,
  "specific_filename": "recibos_energia_indra.xlsx",
  "schema": "raw",
  "table": "web_mm_indra_energia",
  "sp_carga": "ods.sp_cargar_web_hm_indra_energia"
}
```

---

### 9. Generic Autin GDE

**Connection ID**: `generic_autin_gde_<ENV>`

- **Connection Type**: `Generic`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Extra** (JSON):
```json
{
  "proxy": "telefonica01.gp.inet:8080",
  "download_path": "tmp/teleows",
  "local_dir": "tmp/teleows",
  "specific_filename": "Console_GDE_export.xlsx",
  "headless": true,
  "chunk_size": 10000,
  "date_mode": 1,
  "last_n_days": 60,
  "if_exists": "replace",
  "schema": "raw",
  "table": "web_mm_autin_infogeneral",
  "sp_carga": "ods.sp_cargar_gde_tasks",
  "options_to_select": "CM,OPM"
}
```

---

### 10. Generic Autin Dynamic Checklist

**Connection ID**: `generic_autin_dc_<ENV>`

- **Connection Type**: `Generic`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Extra** (JSON):
```json
{
  "proxy": "telefonica01.gp.inet:8080",
  "download_path": "tmp/teleows",
  "local_dir": "tmp/teleows",
  "specific_filename": "DynamicChecklist_SubPM.xlsx",
  "headless": true,
  "options_to_select": ["CM", "OPM"],
  "max_iframe_attempts": 60,
  "max_status_attempts": 60,
  "chunk_size": 10000,
  "date_mode": 2,
  "last_n_days": 5,
  "load_mode": "append",
  "schema": "raw",
  "table": "dynamic_checklist_tasks",
  "sp_carga": "ods.sp_cargar_dynamic_checklist_tasks"
}
```

---

### 11. Generic NetEco

**Connection ID**: `neteco_<ENV>`

- **Connection Type**: `Generic`
- **Login**: `<LOGIN>`
- **Password**: `<PASSWORD>`
- **Extra** (JSON):
```json
{
  "local_dir": "tmp/neteco",
  "specific_filename": "neteco_diario.zip",
  "export_overwrite_files": true,
  "headless": true,
  "local_destination_dir": "tmp/neteco/processed/neteco_diario.xlsx",
  "date_mode": "manual",
  "start_time": "2025-12-18 00:00:00",
  "end_time": "2025-12-18 00:20:00",
  "schema": "raw",
  "table": "web_md_neteco",
  "sp_carga": "ods.sp_cargar_web_md_neteco"
}
```
**Notas**:
- `date_mode`: `manual` usa `start_time`/`end_time`; `auto` usa el dia anterior 00:00:01  23:59:59.

---

## Variables de Airflow (Opcionales)

Las siguientes Variables pueden configurarse para sobrescribir valores del YAML. El formato es: `PREFIX_FIELD_<ENV>` o `PREFIX_FIELD` (generica).

### Variables de PostgreSQL

- `POSTGRES_USER_<ENV>` / `POSTGRES_USER`
- `POSTGRES_PASSWORD_<ENV>` / `POSTGRES_PASSWORD`
- `POSTGRES_HOST_<ENV>` / `POSTGRES_HOST`
- `POSTGRES_PORT_<ENV>` / `POSTGRES_PORT`
- `POSTGRES_DATABASE_<ENV>` / `POSTGRES_DATABASE`

### Variables de SFTP Energia

- `SFTP_ENERGIA_HOST_<ENV>` / `SFTP_ENERGIA_HOST`
- `SFTP_ENERGIA_PORT_<ENV>` / `SFTP_ENERGIA_PORT`
- `SFTP_ENERGIA_USERNAME_<ENV>` / `SFTP_ENERGIA_USERNAME`
- `SFTP_ENERGIA_PASSWORD_<ENV>` / `SFTP_ENERGIA_PASSWORD`
- `SFTP_ENERGIA_REMOTE_DIR_<ENV>` / `SFTP_ENERGIA_REMOTE_DIR`
- `SFTP_ENERGIA_LOCAL_DIR_<ENV>` / `SFTP_ENERGIA_LOCAL_DIR`

**Nota**: `sftp_pago_energia` usa la connection dedicada `sftp_pago_energia_<ENV>`.

### Variables de SFTP Pago Energia

**Nota**: `sftp_pago_energia` usa la connection SFTP `sftp_pago_energia_<ENV>` y puede definirse completa en `Extra`.

- `SFTP_PAGO_ENERGIA_REMOTE_DIR_<ENV>` / `SFTP_PAGO_ENERGIA_REMOTE_DIR`
- `SFTP_PAGO_ENERGIA_LOCAL_DIR_<ENV>` / `SFTP_PAGO_ENERGIA_LOCAL_DIR`
- `SFTP_PAGO_ENERGIA_PROCESSED_DIR_<ENV>` / `SFTP_PAGO_ENERGIA_PROCESSED_DIR`
- `SFTP_PAGO_ENERGIA_ERROR_DIR_<ENV>` / `SFTP_PAGO_ENERGIA_ERROR_DIR`
- `SFTP_PAGO_ENERGIA_NOMBRE_SALIDA_LOCAL_<ENV>` / `SFTP_PAGO_ENERGIA_NOMBRE_SALIDA_LOCAL`
- `SFTP_PAGO_ENERGIA_SCHEMA_<ENV>` / `SFTP_PAGO_ENERGIA_SCHEMA`
- `SFTP_PAGO_ENERGIA_TABLE_<ENV>` / `SFTP_PAGO_ENERGIA_TABLE`
- `SFTP_PAGO_ENERGIA_SP_CARGA_<ENV>` / `SFTP_PAGO_ENERGIA_SP_CARGA`
- `SFTP_PAGO_ENERGIA_DEFAULT_SHEET_<ENV>` / `SFTP_PAGO_ENERGIA_DEFAULT_SHEET`
- `SFTP_PAGO_ENERGIA_FILA_INICIAL_<ENV>` / `SFTP_PAGO_ENERGIA_FILA_INICIAL`
- `SFTP_PAGO_ENERGIA_SUBSETNA_<ENV>` / `SFTP_PAGO_ENERGIA_SUBSETNA`

### Variables de SFTP Base Sitios

- `SFTP_BASE_SITIOS_HOST_<ENV>` / `SFTP_BASE_SITIOS_HOST`
- `SFTP_BASE_SITIOS_PORT_<ENV>` / `SFTP_BASE_SITIOS_PORT`
- `SFTP_BASE_SITIOS_USERNAME_<ENV>` / `SFTP_BASE_SITIOS_USERNAME`
- `SFTP_BASE_SITIOS_PASSWORD_<ENV>` / `SFTP_BASE_SITIOS_PASSWORD`
- `SFTP_BASE_SITIOS_REMOTE_DIR_<ENV>` / `SFTP_BASE_SITIOS_REMOTE_DIR`
- `SFTP_BASE_SITIOS_LOCAL_DIR_<ENV>` / `SFTP_BASE_SITIOS_LOCAL_DIR`
- `SFTP_BASE_SITIOS_SPECIFIC_FILENAME_<ENV>` / `SFTP_BASE_SITIOS_SPECIFIC_FILENAME`
- `SFTP_BASE_SITIOS_SCHEMA_<ENV>` / `SFTP_BASE_SITIOS_SCHEMA`
- `SFTP_BASE_SITIOS_TABLE_<ENV>` / `SFTP_BASE_SITIOS_TABLE`
- `SFTP_BASE_SITIOS_SP_CARGA_<ENV>` / `SFTP_BASE_SITIOS_SP_CARGA`
- `SFTP_BASE_SITIOS_IF_EXISTS_<ENV>` / `SFTP_BASE_SITIOS_IF_EXISTS`

### Variables de SFTP Base Sitios Bitacora

- `SFTP_BASE_SITIOS_BITACORA_HOST_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_HOST`
- `SFTP_BASE_SITIOS_BITACORA_PORT_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_PORT`
- `SFTP_BASE_SITIOS_BITACORA_USERNAME_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_USERNAME`
- `SFTP_BASE_SITIOS_BITACORA_PASSWORD_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_PASSWORD`
- `SFTP_BASE_SITIOS_BITACORA_REMOTE_DIR_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_REMOTE_DIR`
- `SFTP_BASE_SITIOS_BITACORA_LOCAL_DIR_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_LOCAL_DIR`
- `SFTP_BASE_SITIOS_BITACORA_SPECIFIC_FILENAME_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_SPECIFIC_FILENAME`
- `SFTP_BASE_SITIOS_BITACORA_SCHEMA_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_SCHEMA`
- `SFTP_BASE_SITIOS_BITACORA_TABLE_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_TABLE`
- `SFTP_BASE_SITIOS_BITACORA_SP_CARGA_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_SP_CARGA`
- `SFTP_BASE_SITIOS_BITACORA_IF_EXISTS_<ENV>` / `SFTP_BASE_SITIOS_BITACORA_IF_EXISTS`

### Variables de SFTP TOA

- `SFTP_TOA_HOST_<ENV>` / `SFTP_TOA_HOST`
- `SFTP_TOA_PORT_<ENV>` / `SFTP_TOA_PORT`
- `SFTP_TOA_USERNAME_<ENV>` / `SFTP_TOA_USERNAME`
- `SFTP_TOA_PASSWORD_<ENV>` / `SFTP_TOA_PASSWORD`
- `SFTP_TOA_REMOTE_DIR_<ENV>` / `SFTP_TOA_REMOTE_DIR`
- `SFTP_TOA_LOCAL_DIR_<ENV>` / `SFTP_TOA_LOCAL_DIR`
- `SFTP_TOA_SPECIFIC_FILENAME_<ENV>` / `SFTP_TOA_SPECIFIC_FILENAME`
- `SFTP_TOA_SCHEMA_<ENV>` / `SFTP_TOA_SCHEMA`
- `SFTP_TOA_TABLE_<ENV>` / `SFTP_TOA_TABLE`
- `SFTP_TOA_SP_CARGA_<ENV>` / `SFTP_TOA_SP_CARGA`
- `SFTP_TOA_IF_EXISTS_<ENV>` / `SFTP_TOA_IF_EXISTS`

### Variables de Clientes Libres

- `CLIENTES_LIBRES_REMOTE_DIR_<ENV>` / `CLIENTES_LIBRES_REMOTE_DIR`
- `CLIENTES_LIBRES_LOCAL_DIR_<ENV>` / `CLIENTES_LIBRES_LOCAL_DIR`
- `CLIENTES_LIBRES_SPECIFIC_FILENAME_<ENV>` / `CLIENTES_LIBRES_SPECIFIC_FILENAME`
- `CLIENTES_LIBRES_LOCAL_DESTINATION_DIR_<ENV>` / `CLIENTES_LIBRES_LOCAL_DESTINATION_DIR`
- `CLIENTES_LIBRES_SCHEMA_<ENV>` / `CLIENTES_LIBRES_SCHEMA`
- `CLIENTES_LIBRES_TABLE_<ENV>` / `CLIENTES_LIBRES_TABLE`
- `CLIENTES_LIBRES_SP_CARGA_<ENV>` / `CLIENTES_LIBRES_SP_CARGA`

### Variables de SFTP Base Suministros Activos

- `SFTP_BASE_SUMINISTROS_ACTIVOS_REMOTE_DIR_<ENV>` / `SFTP_BASE_SUMINISTROS_ACTIVOS_REMOTE_DIR`
- `SFTP_BASE_SUMINISTROS_ACTIVOS_LOCAL_DIR_<ENV>` / `SFTP_BASE_SUMINISTROS_ACTIVOS_LOCAL_DIR`
- `SFTP_BASE_SUMINISTROS_ACTIVOS_SPECIFIC_FILENAME_<ENV>` / `SFTP_BASE_SUMINISTROS_ACTIVOS_SPECIFIC_FILENAME`
- `SFTP_BASE_SUMINISTROS_ACTIVOS_SCHEMA_<ENV>` / `SFTP_BASE_SUMINISTROS_ACTIVOS_SCHEMA`
- `SFTP_BASE_SUMINISTROS_ACTIVOS_TABLE_<ENV>` / `SFTP_BASE_SUMINISTROS_ACTIVOS_TABLE`
- `SFTP_BASE_SUMINISTROS_ACTIVOS_IF_EXISTS_<ENV>` / `SFTP_BASE_SUMINISTROS_ACTIVOS_IF_EXISTS`
- `SFTP_BASE_SUMINISTROS_ACTIVOS_SHEET_NAME_<ENV>` / `SFTP_BASE_SUMINISTROS_ACTIVOS_SHEET_NAME`
- `SFTP_BASE_SUMINISTROS_ACTIVOS_PAGE_SIZE_<ENV>` / `SFTP_BASE_SUMINISTROS_ACTIVOS_PAGE_SIZE`

### Variables de SFTP DAAS

- `SFTP_DAAS_HOST_<ENV>` / `SFTP_DAAS_HOST`
- `SFTP_DAAS_PORT_<ENV>` / `SFTP_DAAS_PORT`
- `SFTP_DAAS_USERNAME_<ENV>` / `SFTP_DAAS_USERNAME`
- `SFTP_DAAS_PASSWORD_<ENV>` / `SFTP_DAAS_PASSWORD`

### Variables de Teleows

- `TELEOWS_USERNAME_<ENV>` / `TELEOWS_USERNAME`
- `TELEOWS_PASSWORD_<ENV>` / `TELEOWS_PASSWORD`
- `TELEOWS_PROXY_<ENV>` / `TELEOWS_PROXY`
- `TELEOWS_DOWNLOAD_PATH_<ENV>` / `TELEOWS_DOWNLOAD_PATH`
- `TELEOWS_MAX_IFRAME_ATTEMPTS_<ENV>` / `TELEOWS_MAX_IFRAME_ATTEMPTS`
- `TELEOWS_MAX_STATUS_ATTEMPTS_<ENV>` / `TELEOWS_MAX_STATUS_ATTEMPTS`
- `TELEOWS_OPTIONS_TO_SELECT_<ENV>` / `TELEOWS_OPTIONS_TO_SELECT`
- `TELEOWS_EXPORT_OVERWRITE_FILES_<ENV>` / `TELEOWS_EXPORT_OVERWRITE_FILES`
- `TELEOWS_HEADLESS_<ENV>` / `TELEOWS_HEADLESS`

### Variables de Web Indra

- `WEBINDRA_ENERGIA_BASE_URL_<ENV>` / `WEBINDRA_ENERGIA_BASE_URL`
- `WEBINDRA_ENERGIA_LOGIN_PATH_<ENV>` / `WEBINDRA_ENERGIA_LOGIN_PATH`
- `WEBINDRA_ENERGIA_EXPORT_TMPL_<ENV>` / `WEBINDRA_ENERGIA_EXPORT_TMPL`
- `WEBINDRA_ENERGIA_USER_<ENV>` / `WEBINDRA_ENERGIA_USER`
- `WEBINDRA_ENERGIA_PASS_<ENV>` / `WEBINDRA_ENERGIA_PASS`
- `WEBINDRA_ENERGIA_PERIOD_MONTHS_<ENV>` / `WEBINDRA_ENERGIA_PERIOD_MONTHS`
- `WEBINDRA_ENERGIA_TIMEOUT_<ENV>` / `WEBINDRA_ENERGIA_TIMEOUT`
- `WEBINDRA_ENERGIA_MAX_RETRIES_<ENV>` / `WEBINDRA_ENERGIA_MAX_RETRIES`
- `WEBINDRA_ENERGIA_PROXY_<ENV>` / `WEBINDRA_ENERGIA_PROXY`
- `WEBINDRA_ENERGIA_HEADERS_<ENV>` / `WEBINDRA_ENERGIA_HEADERS` (debe ser JSON string)

### Variables de GDE

- `TELEOWS_GDE_DATE_MODE_<ENV>` / `TELEOWS_GDE_DATE_MODE`
- `TELEOWS_GDE_LAST_N_DAYS_<ENV>` / `TELEOWS_GDE_LAST_N_DAYS`
- `TELEOWS_GDE_SPECIFIC_FILENAME_<ENV>` / `TELEOWS_GDE_SPECIFIC_FILENAME`
- `TELEOWS_GDE_LOCAL_DIR_<ENV>` / `TELEOWS_GDE_LOCAL_DIR`
- `TELEOWS_GDE_SCHEMA_<ENV>` / `TELEOWS_GDE_SCHEMA`
- `TELEOWS_GDE_TABLE_<ENV>` / `TELEOWS_GDE_TABLE`
- `TELEOWS_GDE_CHUNKSIZE_<ENV>` / `TELEOWS_GDE_CHUNKSIZE`
- `TELEOWS_GDE_SP_CARGA_<ENV>` / `TELEOWS_GDE_SP_CARGA`

### Variables de Dynamic Checklist

- `TELEOWS_DC_DATE_MODE_<ENV>` / `TELEOWS_DC_DATE_MODE`
- `TELEOWS_DC_LAST_N_DAYS_<ENV>` / `TELEOWS_DC_LAST_N_DAYS`
- `TELEOWS_DC_SPECIFIC_FILENAME_<ENV>` / `TELEOWS_DC_SPECIFIC_FILENAME`
- `TELEOWS_DC_LOCAL_DIR_<ENV>` / `TELEOWS_DC_LOCAL_DIR`
- `TELEOWS_DC_SCHEMA_<ENV>` / `TELEOWS_DC_SCHEMA`
- `TELEOWS_DC_TABLE_<ENV>` / `TELEOWS_DC_TABLE`
- `TELEOWS_DC_LOAD_MODE_<ENV>` / `TELEOWS_DC_LOAD_MODE`
- `TELEOWS_DC_CHUNKSIZE_<ENV>` / `TELEOWS_DC_CHUNKSIZE`
- `TELEOWS_DC_SP_CARGA_<ENV>` / `TELEOWS_DC_SP_CARGA`

### Variables de NetEco

- `NETECO_LOCAL_DIR_<ENV>` / `NETECO_LOCAL_DIR`
- `NETECO_SPECIFIC_FILENAME_<ENV>` / `NETECO_SPECIFIC_FILENAME`
- `NETECO_EXPORT_OVERWRITE_FILES_<ENV>` / `NETECO_EXPORT_OVERWRITE_FILES`
- `NETECO_HEADLESS_<ENV>` / `NETECO_HEADLESS`
- `NETECO_LOCAL_DESTINATION_DIR_<ENV>` / `NETECO_LOCAL_DESTINATION_DIR`
- `NETECO_DATE_MODE_<ENV>` / `NETECO_DATE_MODE`
- `NETECO_START_TIME_<ENV>` / `NETECO_START_TIME`
- `NETECO_END_TIME_<ENV>` / `NETECO_END_TIME`
- `NETECO_SCHEMA_<ENV>` / `NETECO_SCHEMA`
- `NETECO_TABLE_<ENV>` / `NETECO_TABLE`
- `NETECO_SP_CARGA_<ENV>` / `NETECO_SP_CARGA`
- `NETECO_TIMEZONE_<ENV>` / `NETECO_TIMEZONE`

---

## Notas Importantes

1. **Prioridad de configuracion**: Variables > Connections > YAML
2. Si una Variable existe, sobrescribe el valor de la Connection o YAML
3. Las Variables con sufijo `_<ENV>` tienen prioridad sobre las genericas
4. El campo `Extra` en las Connections debe ser un JSON valido
5. Para valores booleanos en Variables, usar `true` o `false` (strings)
6. Para valores numericos, usar numeros directamente o strings numericas
7. Para `HEADERS`, debe ser un JSON string valido

---

## Como Importar Connections

Puedes usar el archivo `airflow_connections_import_dict_<ENV>.json` para importar todas las connections de una vez usando el comando de Airflow CLI:

```bash
airflow connections import airflow_connections_import_dict_<ENV>.json
```
