<h1 align="center">
  🔄 Scraper Integratel
</h1>

<p align="center">
  <strong>Stack de Apache Airflow para ETL offline en servidores Linux</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Airflow-3.1.0-017CEE?logo=apacheairflow" alt="Airflow">
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white" alt="Docker">
  <img src="https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Selenium-Chrome-43B02A?logo=selenium&logoColor=white" alt="Selenium">
</p>

---

## 📑 Índice

1. [Instalación de Docker CE (Requisito previo)](#instalación-de-docker-ce-requisito-previo)
2. [Despliegue offline en el servidor](#despliegue-offline-en-el-servidor-linux-amd64)
3. [Generar el paquete offline (Dev)](#generar-el-paquete-offline-dev)
4. [Configuración y credenciales](#configuración-y-credenciales)
5. [Notas de operación](#notas-de-operación)
6. [DAGs y Schedules](#dags-y-schedules)
7. [Documentación adicional](#documentación-adicional)

---

## Instalación de Docker CE (Requisito previo)

Si el servidor no tiene Docker instalado, consulta la guía completa en **[docs/DOCKER_INSTALL.md](docs/DOCKER_INSTALL.md)**.

---

## Despliegue offline en el servidor (Linux `amd64`)

1. **Copiar imagen tar.gz via sftp a la raiz del directorio**
   ```
   scraper-integratel-offline.tar.gz
   ```

2. **Copiar repositorio completo al servidor**
   Asegúrate de tener el repositorio completo en el servidor con todas las carpetas necesarias.

3. **Importar imágenes sin Internet**
   ```bash
   cd /daas1/analytics
   sudo sh -c "gunzip -c $(pwd)/scraper-integratel-offline.tar.gz | docker load"
   sudo docker images | grep -E 'scraper-integratel|postgres|redis|prom'
   ```
4. **Revisar configuración del proyecto**
   - Crear directorios necesarios:
     ```bash
     mkdir -p ./dags ./logs ./plugins ./config ./monitoring ./proyectos
     ```
   - Crear archivo `.env` con la configuración básica:
     ```bash
     echo -e "AIRFLOW_UID=$(id -u)" > .env
     echo -e "LOG_LEVEL=INFO" >> .env
     ```
5. **Llenar cada carpeta con los contenidos necesarios**
   - `docker-compose.yml`: Debe estar en la raíz del repositorio.
   - `dags/`: Coloca los DAGs que quieras habilitar (si no hay ninguno, deja la carpeta vacía).
   - `proyectos/`: Debe contener todo el codigo base necesario para correr los DAGs (imprescindible para que los DAGs funcionen).
   - `plugins/`: Debe contener plugins personalizados de Airflow (**debe contener `custom_metrics.py` para usar métricas**).
   - `monitoring/`: Debe contener `statsd-mapping.yml` y `prometheus.yml` para el sistema de métricas.

6. **Levantar la plataforma**
   ```bash
   cd /daas1/analytics
   sudo docker compose up -d --pull never
   ```
7. **Verificar servicios**
   ```bash
   curl http://IP_DE_SERVIDOR:9095/api/v2/version
   ```
   Si la red corporativa bloquea el puerto 9095, crea un túnel: `ssh -L 9095:localhost:9095 usuario@servidor`.

8. **Acceso y operaciones**
   - UI de Airflow: `http://<host>:9095` (usuario/clave por defecto: `airflow` / `airflow`).
   - Logs: `sudo docker compose logs -f airflow-worker`.
   - Detener servicios: `sudo docker compose down` (usa `-v` si deseas borrar volúmenes).

### Importar Connections desde JSON

Para cargar múltiples connections de forma masiva:

1. **Crear archivo JSON con las connections** (ejemplo `connections.json`):
   ```json
   {
     "postgres_siom_prod": {
       "conn_type": "postgres",
       "host": "10.226.17.162",
       "port": 5425,
       "schema": "siom_prod",
       "login": "usuario",
       "password": "password123",
       "extra": "{\"application_name\": \"airflow\"}"
     },
     "sftp_daas_prod": {
       "conn_type": "sftp",
       "host": "10.226.17.100",
       "port": 22,
       "login": "usuario_sftp",
       "password": "password_sftp",
       "extra": "{\"default_remote_dir\": \"/daas/prod/\"}"
     }
   }
   ```

2. **Copiar el archivo al directorio del proyecto** (se monta automáticamente):
   ```bash
   cp connections.json /daas1/analytics/config/
   ```

3. **Importar las connections:**
   ```bash
   cd /daas1/analytics
   sudo docker compose exec airflow-webserver airflow connections import /opt/airflow/config/connections.json
   ```

4. **Verificar que se importaron:**
   ```bash
   sudo docker compose exec airflow-webserver airflow connections list
   ```

**Nota:** Si una connection ya existe, será sobrescrita. Para exportar connections existentes:
```bash
sudo docker compose exec airflow-webserver airflow connections export /tmp/backup.json
sudo docker compose cp airflow-webserver:/tmp/backup.json ./backup_connections.json
```

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

### Variables de entorno opcionales
Puedes definir estas variables antes de ejecutar el script para personalizar el comportamiento:

```bash
# Ejemplo: Generar para arquitectura ARM64
TARGET_PLATFORM=linux/arm64 ./generar_paquete_offline.sh

# Ejemplo: Cambiar la etiqueta de la imagen
IMAGE_TAG=v1 ./generar_paquete_offline.sh

# Ejemplo: Cambiar el nombre del archivo de salida
ARCHIVE_NAME=mi-bundle.tar.gz ./generar_paquete_offline.sh

# Ejemplo: Reutilizar imagen existente (no reconstruir)
SKIP_BUILD=1 ./generar_paquete_offline.sh
```

**Variables disponibles:**
- `TARGET_PLATFORM`: Plataforma objetivo (por defecto: `linux/amd64`, opciones: `linux/arm64`, etc.)
- `IMAGE_TAG`: Etiqueta de la imagen Docker (por defecto: `latest`)
- `ARCHIVE_NAME`: Nombre del archivo tar.gz generado (por defecto: `scraper-integratel-offline.tar.gz`)
- `SKIP_BUILD`: Si es `1`, no ejecuta `docker build` y reutiliza una imagen local existente

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

El sistema carga configuración desde múltiples fuentes con la siguiente **prioridad**:

```
Airflow Variables > Airflow Connections > YAML > Variables de entorno
```

### Variables Requeridas

| Variable | Descripción | Valores |
|----------|-------------|---------|
| `ENV_MODE` | Entorno actual | `dev`, `staging`, `prod` |
| `LOGGING_LEVEL` | Nivel de logging | `INFO`, `DEBUG`, `WARNING`, `ERROR` |

### Airflow Connections

Las Connections almacenan credenciales. Formato: `{conn_id}_{env}` (ej: `postgres_siom_dev`).

**Ejemplo PostgreSQL:**
```
Connection Id: postgres_siom_dev
Connection Type: postgres
Host: 10.226.17.162
Port: 5425
Schema: siom_dev
Login: usuario
Password: ********
Extra (JSON): {"application_name":"airflow"}
```

### Uso en código

```python
from energiafacilities.core.utils import load_config

config = load_config(env='dev')
postgres_config = config.get("postgress", {})
```

Para documentación completa del sistema de configuración (secciones mapeadas, auto-descubrimiento, YAML), ver [proyectos/energiafacilities/README.md](proyectos/energiafacilities/README.md#configuración).

---

## Notas de operación

- Ejecuta `AIRFLOW_UID=$(id -u)` antes de `docker compose up` si levantas el stack en otra máquina Linux.
- Para usar Docker sin `sudo`, añade tu usuario al grupo `docker` y vuelve a iniciar sesión.
- Los logs en vivo están disponibles con `sudo docker compose logs -f`.
- Si necesitas reconstruir la imagen (por ejemplo, cambiar Chrome/Chromedriver), vuelve a ejecutar `./generar_paquete_offline.sh` y distribuye el nuevo bundle.

---

## DAGs y Schedules

| DAG | Descripción | Schedule | Frecuencia |
|-----|-------------|----------|------------|
| `DAG_neteco` | ETL NetEco (scraper + transform + load + SP) | `0 */3 * * *` | Cada 3 horas |
| `DAG_neteco_alertas` | Reportes XLSX de alertas NetEco (faltantes y anomalías) | `None` | Manual |
| `DAG_gde` | Scraper y carga de datos GDE | `0 */3 * * *` | Cada 3 horas |
| `DAG_dynamic_checklist` | ETL completo para Dynamic Checklist | `0 2,5,8,11,14,17,20,23 * * *` | Cada 3h (desfasado +2h) |
| `DAG_sftp_energia` | Recibos de energía SFTP (PD/DA) | `0 */3 * * *` | Cada 3 horas |
| `DAG_sftp_pago_energia` | Pagos de energía SFTP | `0 */3 * * *` | Cada 3 horas |
| `DAG_sftp_toa` | Reportes TOA SFTP | `0 */3 * * *` | Cada 3 horas |
| `DAG_clientes_libres` | Clientes libres SFTP | `0 */3 * * *` | Cada 3 horas |
| `DAG_base_sitios` | Base de sitios (base + bitácora) | `0 */3 * * *` | Cada 3 horas |
| `DAG_sftp_base_suministros_activos` | Base suministros activos SFTP | `0 */3 * * *` | Cada 3 horas |
| `DAG_webindra` | Recibos Indra web | `0 */3 * * *` | Cada 3 horas |
| `DAG_cargaglobal` | Carga manual parametrizada | `None` | Manual |
| `DAG_healthcheck_config` | Healthcheck de variables/connections | `None` | Manual |

**Horarios de ejecución automática:**
- **Cada 3 horas (estándar):** 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00
- **Cada 3 horas (desfasado):** 02:00, 05:00, 08:00, 11:00, 14:00, 17:00, 20:00, 23:00

---

## Documentación adicional

| Documento | Descripción |
|-----------|-------------|
| [proyectos/energiafacilities/README.md](proyectos/energiafacilities/README.md) | Documentación del framework y módulos de extracción |
| [docs/DOCKER_INSTALL.md](docs/DOCKER_INSTALL.md) | Guía de instalación de Docker CE |
| [docs/TABLAS_FINALES_INGESTAS.md](docs/TABLAS_FINALES_INGESTAS.md) | Detalle de tablas RAW/ODS por ingesta |
| [proyectos/energiafacilities/sources/reporte_neteco/README_anomalias_consumo.md](proyectos/energiafacilities/sources/reporte_neteco/README_anomalias_consumo.md) | Metodología del reporte de anomalías de consumo |

### Stored Procedures

Los stored procedures están en `db/fase 3/ods/funcion/`. Definición de tablas en `db/fase 3/raw/` y `db/fase 3/ods/tabla/`.

<details>
<summary>Ver tabla SP ↔ DAG</summary>

| DAG | SP(s) | Origen (RAW) | Destino (ODS) |
|-----|-------|--------------|---------------|
| DAG_neteco | `ods.sp_cargar_web_md_neteco` | `raw.web_md_neteco` | `ods.web_hd_neteco`, `ods.web_hd_neteco_diaria` |
| DAG_gde | `ods.sp_cargar_gde_tasks` | `raw.web_mm_autin_infogeneral` | `ods.web_hm_autin_infogeneral` |
| DAG_dynamic_checklist | `ods.sp_cargar_dynamic_checklist_tasks` | `raw.dynamic_checklist_tasks` | Tablas ODS checklist |
| DAG_sftp_energia | `ods.sp_cargar_sftp_hm_consumo_suministro_da/pd` | `raw.sftp_mm_consumo_suministro_da/pd` | `ods.sftp_hm_consumo_suministro` |
| DAG_sftp_pago_energia | `ods.sp_cargar_sftp_hm_pago_energia` | `raw.sftp_mm_pago_energia` | `ods.sftp_hm_pago_energia` |
| DAG_sftp_toa | `ods.sp_cargar_sftp_hd_toa` | `raw.sftp_hd_toa` | `ods.sftp_hd_toa` |
| DAG_clientes_libres | `ods.sp_cargar_sftp_hm_clientes_libres` | `raw.sftp_mm_clientes_libres` | `ods.sftp_hm_clientes_libres` |
| DAG_base_sitios | `ods.sp_cargar_fs_hm_base_de_sitios` | `raw.fs_mm_base_de_sitios` | `ods.fs_hm_base_de_sitios` |
| DAG_webindra | `ods.sp_cargar_web_hm_indra_energia` | `raw.web_mm_indra_energia` | `ods.web_hm_indra_energia` |
| DAG_sftp_base_suministros_activos | (carga directa) | — | `ods.sftp_hm_base_suministros_activos` |

</details>
