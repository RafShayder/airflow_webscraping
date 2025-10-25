# Teleows Scraper – Despliegue Docker + Airflow

Esta guía describe cómo dejar operativa la automatización en un servidor de producción usando contenedores Docker y, opcionalmente, un DAG de Airflow para orquestar los flujos.

---

## 1. Requisitos del servidor

- Docker Engine y Docker Compose v2
- Git
- Credenciales del portal Integratel (usuario/clave)
- Credenciales de la base de datos PostgreSQL donde se almacenarán los datos
- (Opcional) Airflow 2.x o un servicio de orquestación similar, con capacidad para ejecutar contenedores o tareas Python

---

## 2. Clonar el repositorio

```bash
git clone https://github.com/adraguidev/scraper-teleows.git
cd scraper-teleows
```

---

## 3. Configuración de variables (env vars / env.yaml)

El paquete `proyectos/teleows/config.py` carga valores en el siguiente orden de prioridad:
1. Variables de entorno exportadas en el sistema o definidas en Airflow.
2. Variables definidas en `.env` (copia de `proyectos/teleows/env.example`).
3. Valores definidos en `proyectos/teleows/env.yaml` (opcional).

| Variable | Descripción | Default |
| --- | --- | --- |
| `USERNAME` / `PASSWORD` | Credenciales del portal Integratel | **Obligatorio** |
| `DOWNLOAD_PATH` | Carpeta donde Chrome deposita los archivos descargados | `/opt/airflow/proyectos/teleows/temp` en Airflow, `~/Downloads/scraper_downloads` en local |
| `MAX_IFRAME_ATTEMPTS` / `MAX_STATUS_ATTEMPTS` | Reintentos para localizar iframes y estados de exportación | `60` |
| `OPTIONS_TO_SELECT` | Lista (separada por comas) para los filtros de Task Type | `CM,OPM` |
| `DATE_MODE` | `1` = fechas manuales, `2` = último mes | `2` |
| `DATE_FROM` / `DATE_TO` | Rango de fechas manual cuando `DATE_MODE=1` | `2025-09-01` / `2025-09-10` |
| `GDE_OUTPUT_FILENAME` / `DYNAMIC_CHECKLIST_OUTPUT_FILENAME` | Nombre personalizado para los archivos exportados | vacíos (usa el nombre original) |
| `EXPORT_OVERWRITE_FILES` | `true` reemplaza archivos existentes, `false` genera un nombre incremental | `true` |
| `TELEOWS_ENV` | Perfil opcional para seleccionar una sección dentro de `env.yaml` | `default` |

> La carpeta indicada en `DOWNLOAD_PATH` se crea automáticamente si no existe (cuando los permisos del sistema lo permiten).

### 3.1 `.env` tradicional

```bash
cp proyectos/teleows/env.example proyectos/teleows/.env
```

Edita los valores según tu entorno. Cuando el contenedor se construye, el archivo `.env` se monta automáticamente y el módulo `config.py` lo carga.

### 3.2 `env.yaml` opcional

Si prefieres manejar configuraciones por entorno (dev/prod), crea `proyectos/teleows/env.yaml` a partir de `env.yaml.example`:

```bash
cp proyectos/teleows/env.yaml.example proyectos/teleows/env.yaml
```

Define secciones por perfil (`default`, `prod`, etc.) y selecciona cuál usar con la variable `TELEOWS_ENV`. Los valores del YAML rellenan únicamente las variables que no existan en el entorno o en `.env`.
Las claves se convierten automáticamente a mayúsculas, por lo que puedes escribirlas en minúsculas dentro del YAML.

---

## 4. Levantar la infraestructura Docker

Construir y desplegar los servicios:

```bash
docker compose build
docker compose up -d
```

Servicios incluidos:
- `postgres` (imagen oficial `postgres:17`) con inicialización automática ejecutando `sql/init.sql` y `sql/create_gde_table.sql`.
- `scraper` (imagen `python:3.13-slim`) con Selenium, Chrome for Testing y dependencias listas para ejecutar los scripts.

Verificar estado:
```bash
docker compose ps
```

Detener y limpiar (cuando sea necesario):
```bash
docker compose down -v
```

---

## 5. Ejecución manual dentro del contenedor

Para lanzar los scrapers desde el host:

```bash
# GDE
docker compose exec scraper python GDE.py

# Dynamic Checklist
docker compose exec scraper python dynamic_checklist.py
```

El contenedor `scraper` está configurado para permanecer activo (`tail -f /dev/null`), por lo que se recomienda ejecutar los scripts manualmente o desde Airflow.

Los archivos descargados se guardan en la ruta indicada por `DOWNLOAD_PATH`. En los ejemplos de Docker Compose, esa carpeta corresponde al volumen `./temp` montado en `/app/temp`.

---

## 6. Integración con Airflow

### 6.1 Dependencias requeridas en Airflow

Si Airflow se ejecuta en contenedores separados, asegúrate de que el entorno donde corran los tasks tenga:

- Python 3.10+ (Airflow 2.x)
- Dependencias del scraper (`selenium`, `webdriver-manager`, `python-dotenv`)
- Acceso a las mismas variables de entorno (usar `Env Vars`, `Airflow Connections/Variables` o archivos `.env` montados)
- Acceso al directorio de descargas (montar `temp` como volumen compartido)

### 6.2 Configurar credenciales desde la UI de Airflow

Puedes gestionar las credenciales sin entrar al contenedor:

- **Connection recomendada**: crea una conexión `teleows_portal` (`Admin ▸ Connections ▸ +`). Usa `Conn Type = Generic`, coloca el usuario en `Login`, la contraseña en `Password` y en `Extras` (JSON) agrega claves opcionales:
  ```json
  {
    "download_path": "/opt/airflow/tmp/teleows",
    "options_to_select": "CM,OPM",
    "gde_output_filename": "gde_report.xlsx",
    "export_overwrite_files": true
  }
  ```
  Los DAGs leerán `login`, `password` y cualquier clave extra compatible (`download_path`, `max_iframe_attempts`, `date_mode`, etc.).
- **Variables** (opcional o complementario): añade Variables desde `Admin ▸ Variables ▸ +` con las llaves:
  - `TELEOWS_USERNAME`
  - `TELEOWS_PASSWORD`
  - `TELEOWS_DOWNLOAD_PATH`
  - `TELEOWS_OPTIONS_TO_SELECT`
  - `TELEOWS_DATE_MODE`
  - `TELEOWS_DATE_FROM`
  - `TELEOWS_DATE_TO`
  - `TELEOWS_MAX_IFRAME_ATTEMPTS`
  - `TELEOWS_MAX_STATUS_ATTEMPTS`
  - `TELEOWS_OUTPUT_FILENAME` (usado por ambos scrapers para renombrar el archivo)
  - `TELEOWS_EXPORT_OVERWRITE` (`true`/`false`)

Los DAGs cargan primero la conexión, luego las variables y, si no hay valores definidos, mantienen los provenientes de `.env`/`env.yaml`.

### 6.3 Ejemplo de DAG

Los DAGs `DAG_gde.py` y `DAG_dynamic_checklist.py` pueden importar directamente las funciones expuestas por el paquete:

```python
from teleows import run_gde, run_dynamic_checklist
```

### 6.4 Estrategia recomendada

1. **Task 1** – Ejecutar scraper GDE.
2. **Task 2** – Ejecutar scraper Dynamic Checklist (si aplica).
3. (Opcional) Procesar o mover los archivos descargados a almacenamiento de largo plazo.

Cada task puede invocar los scripts dentro del contenedor usando un `BashOperator`, `DockerOperator` o llamando a las funciones `run_gde` / `run_dynamic_checklist` desde un `PythonOperator`.

---

## 7. Logs y monitoreo

- Para ver logs en vivo: `docker compose logs -f scraper`.
- Dentro de Airflow, revisa los logs de cada task.
- En caso de errores Selenium, habilita capturas o aumenta `MAX_IFRAME_ATTEMPTS`.
- Asegúrate de que Chrome for Testing pueda descargarse (requiere acceso a internet durante `docker compose build`).

---

## 8. Seguridad y mantenimiento

- Protege el archivo `.env` (contiene credenciales).
- Mantén la imagen actualizada reconstruyendo periódicamente (`docker compose build --no-cache`).
- Respaldar el volumen `postgres_data` antes de upgrades.
- Documenta cualquier cambio en scripts Selenium para mantener consistencia con Airflow.

---

## 9. Soporte

Si aparece un error:
- **Login falla**: revisar credenciales y posibles captchas.
- **Tabla inexistente**: verificar que `postgres` se inicializó correctamente (logs `docker compose logs postgres`).
- **Chrome no inicia**: reconstruir la imagen para obtener versiones actualizadas.

Con estos pasos, el equipo de operaciones puede levantar el entorno, programar ejecuciones en Airflow y mantener los reportes actualizados de forma automatizada.***
