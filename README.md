# Guía de despliegue con Docker y Airflow

Esta guía explica, paso a paso, cómo preparar el entorno, construir la imagen con Chrome/Chromedriver incluido y dejar los contenedores listos para producción.


## 1. Chrome y ChromeDriver requeridos

La imagen se construye sobre `apache/airflow:3.1.0-python3.12` y espera encontrar los binarios de Chrome y Chromedriver en la raíz del repositorio (misma carpeta del `Dockerfile`):

- `chrome_140_amd64.deb`
- `chromedriver-linux64.zip`

Estos archivos ya están versionados. 

---

## 2. Configuración de variables (`.env` y `env.yaml`)

El módulo `proyectos/teleows/config.py` carga variables en este orden:
1. Variables de entorno (por ejemplo, definidas en Airflow o exportadas en la shell).
2. Variables del archivo `.env`.
3. Valores de `env.yaml`.

Variables clave:

| Variable | Descripción | Default |
| --- | --- | --- |
| `USERNAME` / `PASSWORD` | Credenciales teleows | **Obligatorio** |
| `DOWNLOAD_PATH` | Carpeta donde Chrome guarda las descargas | `./tmp` |
| `MAX_IFRAME_ATTEMPTS` / `MAX_STATUS_ATTEMPTS` | Reintentos Selenium | `60` |
| `OPTIONS_TO_SELECT` | Task Types separados por comas | `CM,OPM` |
| `DATE_MODE` | `1` = fechas manuales, `2` = último mes | `2` |
| `DATE_FROM` / `DATE_TO` | Rango manual (cuando `DATE_MODE=1`) | `2025-09-01` / `2025-09-10` |
| `GDE_OUTPUT_FILENAME` / `DYNAMIC_CHECKLIST_OUTPUT_FILENAME` | Nombre alternativo de los archivos exportados | vacío |
| `EXPORT_OVERWRITE_FILES` | Sobrescribir (`true`) o versionar (`false`) | `true` |
| `TELEOWS_ENV` | Perfil a usar dentro de `env.yaml` | `default` |

Pasos recomendados:

```bash
cp proyectos/teleows/env.example proyectos/teleows/.env
cp proyectos/teleows/env.yaml.example proyectos/teleows/env.yaml  # opcional
```

Completa el `.env` con las credenciales mínimas. El `env.yaml` sirve para sobreescribir valores por entorno; define secciones (`default`, `prod`, etc.) y elige la activa con `TELEOWS_ENV`.

> La ruta del `DOWNLOAD_PATH` se crea automáticamente en ejecución si los permisos lo permiten. Si Airflow corre externo, asegúrate de montar el mismo volumen en cada worker.

---

## 3. Credenciales en Airflow Connections

Para producción es preferible no dejar credenciales en texto plano. Configura una **Connection** en la UI de Airflow:

- `Admin ▸ Connections ▸ +`
- `Conn Id`: `teleows_portal`
- `Conn Type`: `Generic`
- `Login`: usuario teleows
- `Password`: contraseña teleows
- `Extras` (JSON opcional):
  ```json
   {
   "download_path": "./tmp",
   "max_iframe_attempts": 60,
   "max_status_attempts": 60,
   "date_mode": 2,
   "date_from": "2025-09-01",
   "date_to": "2025-09-10",
   "options_to_select": "CM,OPM",
   "gde_output_filename": "Console_GDE_export.xlsx",
   "dynamic_checklist_output_filename": "DynamicChecklist_SubPM.xlsx",
   "export_overwrite_files": true,
   "headless": true
   }
  ```

Los DAGs consumen primero esta conexión (`login`, `password` y claves de `extras`) y, solo si algún dato falta, consultan Variables o el `.env`.

Variables de Airflow útiles (opcional):

- `TELEOWS_USERNAME`, `TELEOWS_PASSWORD`
- `TELEOWS_DOWNLOAD_PATH`
- `TELEOWS_OPTIONS_TO_SELECT`
- `TELEOWS_DATE_MODE`, `TELEOWS_DATE_FROM`, `TELEOWS_DATE_TO`
- `TELEOWS_MAX_IFRAME_ATTEMPTS`, `TELEOWS_MAX_STATUS_ATTEMPTS`
- `TELEOWS_GDE_OUTPUT_FILENAME`, `TELEOWS_DYNAMIC_CHECKLIST_OUTPUT_FILENAME`
- `TELEOWS_EXPORT_OVERWRITE`

---

## 6. Construir la imagen y levantar Docker Compose

1. Exporta el usuario de Airflow (evita archivos con dueño `root`):

   ```bash
   export AIRFLOW_UID=$(id -u)
   ```

2. Construye la imagen personalizada (instala Chrome y dependencias Python):

   ```bash
   docker compose build
   ```

   - Si cambiaste el `.deb` o el `requirements.txt`, añade `--no-cache`.

3. Inicia todos los servicios en segundo plano:

   ```bash
   docker compose up -d
   ```

   Servicios incluidos: `postgres`, `redis`, `airflow-init`, webserver/API (`airflow-apiserver`), `scheduler`, `worker`, `triggerer`, `dag-processor`, y el stack de métricas (`statsd-exporter`, `prometheus`).

4. Verifica que todo esté saludable:

   ```bash
   docker compose ps
   docker compose logs -f airflow-apiserver  # opcional
   ```

5. Accede a la UI de Airflow en `http://<host>:9095`. Usuario y contraseña por defecto: `airflow` / `airflow`.

Para detener el stack:

```bash
docker compose down -v
```

---

## 7. Ejecutar los scrapers

Desde la máquina host puedes entrar al contenedor principal (`airflow-worker`) y lanzar los scripts:

```bash
# Shell dentro del worker
docker compose exec airflow-worker bash

# Ejecutar scrapers
python proyectos/teleows/GDE.py
python proyectos/teleows/dynamic_checklist.py
```

Si los scrapers se orquestan vía DAGs, revisa `dags/DAG_gde.py` y `dags/DAG_dynamic_checklist.py`, que importan:

```python
from teleows import run_gde, run_dynamic_checklist
```

Ambos DAGs primero intentan leer la conexión `teleows_portal`, luego Variables, y finalmente caen en `.env` / `env.yaml`.

Los archivos descargados quedan en la ruta definida por `DOWNLOAD_PATH`, montada por defecto en `./temp`.

---

## 8. Monitoreo, seguridad y mantenimiento

- Logs en vivo: `docker compose logs -f airflow-worker`.
- Postgres se crea con el volumen `postgres-db-volume`; respáldalo antes de actualizaciones.
- Mantén el `.env` fuera de repositorios públicos y con permisos restringidos.
- Si Chrome deja de abrir, reconstruye la imagen (`docker compose build --no-cache`) para descargar versiones recientes.
- Ajusta `MAX_IFRAME_ATTEMPTS` o los valores de `extras` en la conexión si se detectan timeouts.

Con estas instrucciones se podrá reproducir el entorno Docker, instalar los binarios requeridos y administrar las credenciales desde Airflow.
