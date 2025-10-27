# Airflow DAGs 

Stack de Apache Airflow con Selenium/Chrome preparado para ejecutarse en servidores Linux `amd64` sin acceso a Internet.

---

## Despliegue offline en el servidor (Linux `amd64`)

### Archivos que deben estar en el servidor
- Este repositorio completo, con las siguientes carpetas preparadas:
  - `docker-compose.yml`
  - `dags/` (DAGs que quieras habilitar; si no hay ninguno, deja la carpeta vacía)
  - `proyectos/` (código de los scrapers; imprescindible para que los DAGs funcionen)
  - `config/` (archivos de configuración adicionales; puede quedar vacía si no se usa)
  - `plugins/` (plugins personalizados de Airflow; vacía si no utilizas plugins)
  - `logs/` (se genera en tiempo de ejecución; puede existir vacía o dejar que la cree Docker)
- Bundle de imágenes `scraper-integratel-offline.tar.gz`.
- Archivo `.env` en la raíz del repositorio con al menos:
  ```bash
  # Variable mínima para Airflow
  AIRFLOW_UID=50000
  ```

### Pasos
1. **Copiar artefactos**
   ```bash
   scp scraper-integratel-offline.tar.gz usuario@servidor:/home/usuario/
   scp -r scraper-integratel usuario@servidor:/home/usuario/
   ```
2. **Importar imágenes sin Internet**
   ```bash
   ssh usuario@servidor
   cd /home/usuario
   sudo gunzip -c scraper-integratel-offline.tar.gz | sudo docker load
   sudo docker images | grep -E 'scraper-integratel|postgres|redis|prom'
   ```
3. **Revisar configuración del proyecto**
   - Confirma que `/home/usuario/scraper-integratel/.env` contiene `AIRFLOW_UID=50000`.
   - Ajusta propietarios si es necesario (`sudo chown -R usuario:usuario scraper-integratel`).
4. **Levantar la plataforma**
   ```bash
   cd /home/usuario/scraper-integratel
   sudo docker compose up -d --pull never
   sudo docker compose ps
   ```
5. **Verificar servicios**
   ```bash
   curl http://localhost:9095/api/v2/version
   ```
   Si la red corporativa bloquea el puerto 9095, crea un túnel: `ssh -L 9095:localhost:9095 usuario@servidor`.
6. **Acceso y operaciones**
   - UI de Airflow: `http://<host>:9095` (usuario/clave por defecto: `airflow` / `airflow`).
   - Logs: `sudo docker compose logs -f airflow-worker`.
   - Detener servicios: `sudo docker compose down` (usa `-v` si deseas borrar volúmenes).

---

## Generar el paquete offline (Dev)

### Requisitos locales
- Docker Engine 24+ con soporte `buildx`.
- Docker Compose v2.
- Espacio libre aproximado: 6 GB (imágenes + bundle).
- Archivos binarios presentes en la raíz del repo (`chrome_140_amd64.deb`, `chromedriver-linux64.zip`).

### Comando
```bash
./generar_paquete_offline.sh
```

Variables útiles:
- `TARGET_PLATFORM=linux/arm64` (default `linux/amd64`).
- `IMAGE_TAG=v1` cambia la etiqueta de la imagen `scraper-integratel`.
- `ARCHIVE_NAME=mi-bundle.tar.gz` cambia el nombre del archivo final.
- `SKIP_BUILD=1` reutiliza una imagen local existente y evita ejecutar `docker build`.

Salida típica:
```
========================================
  Generador de paquete offline
========================================
Plataforma objetivo: linux/amd64
Imagen de aplicación: scraper-integratel:latest
Archivo de salida   : scraper-integratel-offline.tar.gz

[1/5] Construyendo ...
[2/5] Preparando imágenes oficiales ...
[3/5] Verificando arquitecturas ...
[4/5] Generando scraper-integratel-offline.tar.gz ...
[5/5] Paquete listo
```

---

## Configuración y credenciales

El módulo `proyectos/teleows/config.py` aplica la configuración en esta cascada:

1. **Variables de entorno** del proceso Airflow (definidas en `docker-compose.yml`, Variables de Airflow o Secrets).
2. **`proyectos/teleows/.env`** (opcional).
3. **`proyectos/teleows/env.yaml`** (opcional, perfiles por entorno).

Si `.env` o `env.yaml` no existen, se omiten sin error; basta con configurar la Connection y las variables de entorno necesarias.

### Connection `teleows_portal`
Define una Connection (Admin ▸ Connections) con las credenciales del portal y los parámetros del scraper. Ejemplo completo:

```json
{
  "download_path": "./tmp",
  "options_to_select": "CM,OPM",
  "date_mode": 2,
  "date_from": "2025-09-01",
  "date_to": "2025-09-10",
  "max_iframe_attempts": 60,
  "max_status_attempts": 60,
  "download_wait_seconds": 5,
  "poll_interval_seconds": 5,
  "gde_output_filename": "Console_GDE_export.xlsx",
  "dynamic_checklist_output_filename": "DynamicChecklist_SubPM.xlsx",
  "export_overwrite_files": true,
  "headless": true,
}
```

- `date_mode`: `1` aplica el rango manual definido en `date_from`/`date_to`; `2` calcula el rango automático (por ejemplo, último periodo). Deja el valor predeterminado (`2`) salvo que necesites forzar fechas específicas.
- Las credenciales del portal se cargan desde los campos nativos de la Connection (`Login` y `Password`); no es necesario duplicarlas en `Extras`.

### Variables opcionales de Airflow
- `TELEOWS_USERNAME`, `TELEOWS_PASSWORD`
- `TELEOWS_DOWNLOAD_PATH`
- `TELEOWS_OPTIONS_TO_SELECT`
- `TELEOWS_DATE_MODE`, `TELEOWS_DATE_FROM`, `TELEOWS_DATE_TO`
- `TELEOWS_MAX_IFRAME_ATTEMPTS`, `TELEOWS_MAX_STATUS_ATTEMPTS`
- `TELEOWS_GDE_OUTPUT_FILENAME`, `TELEOWS_DYNAMIC_CHECKLIST_OUTPUT_FILENAME`
- `TELEOWS_EXPORT_OVERWRITE`, `TELEOWS_HEADLESS`
- `TELEOWS_ENV`, `TELEOWS_DOWNLOAD_WAIT_SECONDS`, `TELEOWS_POLL_INTERVAL_SECONDS`

### Uso opcional de `.env` y `env.yaml`
- `proyectos/teleows/.env`: útil para valores locales o credenciales que no quieras exponer en Connections; crea el archivo desde `env.example` y personaliza solo lo necesario.
- `proyectos/teleows/env.yaml`: permite múltiples perfiles (`default`, `prod`, etc.) y seleccionar uno mediante `teleows_env`.

Aun cuando se use la Connection, mantén `AIRFLOW_UID=50000` en el `.env` de la raíz del proyecto para evitar problemas de permisos en los volúmenes Docker.

---

## Notas de operación

- Ejecuta `AIRFLOW_UID=$(id -u)` antes de `docker compose up` si levantas el stack en otra máquina Linux.
- Para usar Docker sin `sudo`, añade tu usuario al grupo `docker` y vuelve a iniciar sesión.
- Los logs en vivo están disponibles con `sudo docker compose logs -f`.
- Si necesitas reconstruir la imagen (por ejemplo, cambiar Chrome/Chromedriver), vuelve a ejecutar `./generar_paquete_offline.sh` y distribuye el nuevo bundle.
