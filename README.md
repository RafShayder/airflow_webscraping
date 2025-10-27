# Airflow + Selenium Scraper

Stack de Apache Airflow con Selenium/Chrome listo para ejecutarse en servidores Linux `amd64`, incluso sin acceso a Internet. Este documento consolida la información de preparación, empaquetado offline y despliegue en producción.

## Requisitos locales

- Docker Engine 24+ con soporte `buildx`.
- Docker Compose v2.
- Espacio disponible: ~6 GB para la imagen + bundle offline.
- Archivos binarios presentes en la raíz del repo (ya versionados):
  - `chrome_140_amd64.deb`
  - `chromedriver-linux64.zip`

## Configuración de variables

El módulo `proyectos/teleows/config.py` lee la configuración en este orden:

1. Variables de entorno del proceso Airflow.
2. Archivo `.env` dentro de `proyectos/teleows/`.
3. Archivo `env.yaml` (perfiles por entorno).

Pasos sugeridos:

```bash
cp proyectos/teleows/env.example proyectos/teleows/.env
cp proyectos/teleows/env.yaml.example proyectos/teleows/env.yaml  # opcional
```

Completa al menos las credenciales requeridas (usuario/contraseña, rutas de descarga, etc.). En Airflow puedes definir una Connection `teleows_portal` para centralizar credenciales y overrides:

```json
{
  "download_path": "./tmp",
  "max_iframe_attempts": 60,
  "max_status_attempts": 60,
  "date_mode": 2,
  "options_to_select": "CM,OPM",
  "export_overwrite_files": true,
  "headless": true
}
```

Variables opcionales expuestas como Variables de Airflow:

- `TELEOWS_USERNAME`, `TELEOWS_PASSWORD`
- `TELEOWS_DOWNLOAD_PATH`
- `TELEOWS_OPTIONS_TO_SELECT`
- `TELEOWS_DATE_MODE`, `TELEOWS_DATE_FROM`, `TELEOWS_DATE_TO`
- `TELEOWS_MAX_IFRAME_ATTEMPTS`, `TELEOWS_MAX_STATUS_ATTEMPTS`
- `TELEOWS_GDE_OUTPUT_FILENAME`, `TELEOWS_DYNAMIC_CHECKLIST_OUTPUT_FILENAME`
- `TELEOWS_EXPORT_OVERWRITE`

## Generar el paquete offline (un único `.tar.gz`)

El script `generar_paquete_offline.sh` construye la imagen personalizada `scraper-integratel:latest`, descarga las imágenes oficiales necesarias (`postgres`, `redis`, `prometheus`, `statsd-exporter`) en `linux/amd64` y crea **un solo archivo**: `scraper-integratel-offline.tar.gz`.

```bash
./generar_paquete_offline.sh
```

Variables opcionales:

- `TARGET_PLATFORM=linux/arm64` para otro objetivo (por defecto `linux/amd64`).
- `IMAGE_TAG=v1` para etiquetar la imagen.
- `ARCHIVE_NAME=mi-bundle.tar.gz` para cambiar el nombre del archivo.
- `SKIP_BUILD=1` reutiliza la imagen local y evita ejecutar `docker build`.

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

## Despliegue en servidor sin Internet

1. **Copiar el bundle**  
   ```bash
   scp scraper-integratel-offline.tar.gz usuario@servidor:/home/usuario/
   ```

2. **Importar imágenes**  
   ```bash
   ssh usuario@servidor
   cd /home/usuario
   sudo gunzip -c scraper-integratel-offline.tar.gz | sudo docker load
   ```

3. **Verificar arquitectura**  
   ```bash
   sudo docker image inspect scraper-integratel:latest --format '{{.Architecture}}'
   # Debe mostrar: amd64
   ```

4. **Configurar proyecto**  
   - Copia `docker-compose.yml`, `dags/`, `proyectos/`, `config/`, `plugins/`, `.env`.
   - Ajusta permisos en el host según sea necesario (`sudo chown -R usuario:usuario ...`).

5. **Levantar servicios**  
   ```bash
   cd /home/usuario/scraper-integratel
   sudo docker compose up -d --pull never
   sudo docker compose ps
   ```

6. **Acceder a Airflow**  
   - UI: `http://<host>:9095` (usuario/clave por defecto: `airflow` / `airflow`).

7. **Ejecutar scrapers manualmente (opcional)**  
   ```bash
   sudo docker compose exec airflow-worker bash
   python proyectos/teleows/GDE.py
   python proyectos/teleows/dynamic_checklist.py
   ```

8. **Logs y mantenimiento**  
   ```bash
   sudo docker compose logs -f
   sudo docker compose logs airflow-worker
   sudo docker compose down         # detener
   sudo docker compose down -v      # detener y borrar volúmenes
   ```

## Notas de operación

- Establece `AIRFLOW_UID=$(id -u)` antes de ejecutar `docker compose up` para evitar archivos con dueño `root`.
- Si deseas ejecutar Docker sin `sudo`, añade tu usuario al grupo `docker`:
  ```bash
  sudo usermod -aG docker $USER
  exit  # cierra sesión y vuelve a entrar
  ```
- Si necesitas reconstruir con dependencias nuevas (por ejemplo, otro Chrome), vuelve a ejecutar `./generar_paquete_offline.sh`.
- Ajusta parámetros de Selenium en `.env` o en la Connection `teleows_portal` cuando cambien fechas, descargas o flujos.

Con este flujo se dispone de un único archivo `.tar.gz` para distribuir el stack completo y desplegarlo en entornos desconectados.
