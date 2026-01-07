# Airflow DAG

Stack de Apache Airflow para ejecutarse en servidores Linux `amd64` sin acceso a Internet.

---

## 📑 Índice

1. [Instalación de Docker CE (Requisito previo)](#instalación-de-docker-ce-requisito-previo)
2. [Despliegue offline en el servidor](#despliegue-offline-en-el-servidor-linux-amd64)
3. [Generar el paquete offline (Dev)](#generar-el-paquete-offline-dev)
4. [Configuración y credenciales](#configuración-y-credenciales)
5. [Notas de operación](#notas-de-operación)
6. [Tablas finales por ingesta](#tablas-finales-por-ingesta)
7. [Stored Procedures (SP) en `db/`](#stored-procedures-sp-en-db)

---

## Instalación de Docker CE (Requisito previo)

Si el servidor no tiene Docker instalado, sigue estos pasos para instalarlo sin conexión a Internet.

### Requisitos previos
- Servidor con **RHEL 9.x (x86_64)** sin conexión a Internet.
- Una máquina con Internet para descargar los paquetes.
- Acceso al servidor mediante SFTP.

### 1. Descargar los paquetes RPM en una máquina con Internet

```bash
mkdir -p ~/docker_rpms_x86_64
cd ~/docker_rpms_x86_64

curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/containerd.io-1.7.28-2.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/docker-ce-28.0.0-1.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/docker-ce-cli-28.0.0-1.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/docker-buildx-plugin-0.29.1-1.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/docker-compose-plugin-2.29.7-1.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/docker-ce-rootless-extras-28.0.0-1.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/container-selinux-2.237.0-2.el9_6.noarch.rpm
```

### 2. Crear un paquete comprimido

```bash
cd ..
tar czf docker_rpms_x86_64.tar.gz docker_rpms_x86_64
```

Transferir el archivo al servidor:

```bash
scp docker_rpms_x86_64.tar.gz usuario@ip_servidor:/home/usuario/
```

### 3. Extraer los RPM en el servidor

```bash
cd ~
tar xzf docker_rpms_x86_64.tar.gz
cd docker_rpms_x86_64
ls -1
```

### 4. Instalar Docker CE desde los paquetes locales

```bash
sudo dnf install -y --disablerepo='*' --skip-broken ./*.rpm
```

### 5. Habilitar y arrancar Docker

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now docker
sudo systemctl status docker
```

### 6. Verificar la instalación

```bash
docker --version
sudo docker info
```

### 7. Limpieza opcional

```bash
sudo dnf clean all
rm -rf ~/docker_rpms_x86_64
```

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

El módulo `proyectos/energiafacilities/core/utils.py` proporciona la función `load_config()` que carga la configuración desde múltiples fuentes con la siguiente **prioridad**:

1. **Airflow Variables** (mayor prioridad)
2. **Airflow Connections** 
3. **YAML** (`config/config_{env}.yaml`)
4. **Variables de entorno** (usadas para reemplazo en YAML con `${VAR_NAME}`)

### Prioridad de configuración

```
Variables > Connection > YAML > Variables de entorno
```

Si tienes el mismo valor en múltiples fuentes, siempre ganará la Variable de Airflow.

### Airflow Connections

Las Connections almacenan credenciales y configuración de conexión. Ejemplo para PostgreSQL:

**Connection en Airflow (Admin → Connections):**
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

**En tu código:**
```python
from energiafacilities.core.utils import load_config

config = load_config(env='dev')
postgres_config = config.get("postgress", {})
# postgres_config ahora contiene: host, port, database, user, password, etc.
```

### Airflow Variables

Las Variables permiten sobrescribir valores específicos. Se buscan con prefijo basado en el nombre de la sección:

**Variables en Airflow (Admin → Variables):**
```
ENV_MODE = "dev"
LOGGING_LEVEL = "INFO"
POSTGRES_HOST_DEV = "10.226.17.162"
POSTGRES_PORT_DEV = "5425"
TELEOWS_GDE_USERNAME_DEV = "usuario"  # Variables específicas para GDE
TELEOWS_DC_USERNAME_DEV = "usuario"  # Variables específicas para Dynamic Checklist
```

### Auto-descubrimiento

Para **nuevos módulos** que no estén registrados en `section_mapping`, el sistema usa **auto-descubrimiento**:

1. **Connection**: Busca `{nombre_seccion}_{env}` (ej: `mi_api_dev`) o `{nombre_seccion}` (genérica)
2. **Variables**: Busca con prefijo `{NOMBRE_SECCION}_` (ej: `MI_API_HOST_DEV`)

**Ejemplo con nueva sección `mi_api_test`:**
```python
config = load_config(env='dev')
mi_api_config = config.get("mi_api_test", {})
# El sistema automáticamente busca:
# - Connection: mi_api_test_dev
# - Variables: MI_API_TEST_*
```

No necesitas modificar `utils.py` para agregar nuevas secciones; el auto-descubrimiento funciona automáticamente.

### Configuración YAML

Las configuraciones base están en `proyectos/energiafacilities/config/config_{env}.yaml`:

```yaml
postgress:
  host: "10.226.17.162"
  port: 5425
  user: "${POSTGRES_USER}"
  password: "${POSTGRES_PASS}"
```

Las variables de entorno se reemplazan automáticamente usando `${VAR_NAME}`.

### Secciones mapeadas

Algunas secciones tienen mapeo explícito en `section_mapping`:
- `postgress` → Connection: `postgres_siom_{env}`, Variables: `POSTGRES_*`
- `teleows` → Connection: `generic_autin_shared_{env}` (opcional, no recomendada), Variables: `TELEOWS_*`
- `gde` → Connection: `generic_autin_gde_{env}`, Variables: `TELEOWS_GDE_*`
- `dynamic_checklist` → Connection: `generic_autin_dc_{env}`, Variables: `TELEOWS_DC_*`
- `sftp_energia_c` → Connection: `sftp_energia_{env}`, Variables: `SFTP_ENERGIA_*`
- `sftp_energia` → Connection: `None` (usa `sftp_energia_c`), Variables: `SFTP_ENERGIA_*`
- `sftp_daas_c` → Connection: `sftp_daas_{env}`, Variables: `SFTP_DAAS_*`
- `sftp_base_sitios` → Connection: `sftp_base_sitios_{env}`, Variables: `SFTP_BASE_SITIOS_*`
- `sftp_base_sitios_bitacora` → Connection: `sftp_base_sitios_bitacora_{env}`, Variables: `SFTP_BASE_SITIOS_BITACORA_*`
- `webindra_energia` → Connection: `http_webindra_{env}`, Variables: `WEBINDRA_ENERGIA_*`
- `clientes_libres` → Connection: `sftp_clientes_libres_{env}`, Variables: `CLIENTES_LIBRES_*`

**Nota**: La connection `generic_autin_shared_{env}` es opcional y no recomendada. Los módulos GDE y Dynamic Checklist tienen sus propias connections específicas que incluyen toda la configuración necesaria.

Las secciones no mapeadas (como `sftp_pago_energia`) usan auto-descubrimiento basado en convenciones.

### Ejemplo completo

```python
from energiafacilities.core.utils import load_config

# Carga config para entorno 'dev' (determinado automáticamente si no se especifica)
config = load_config(env='dev')

# Obtener configuración de PostgreSQL
postgres = config.get("postgress", {})

# Obtener configuración de nuevo módulo (auto-descubrimiento)
mi_api = config.get("mi_api_test", {})
# Busca automáticamente: mi_api_test_dev (Connection) y MI_API_TEST_* (Variables)
```

### Variables Requeridas

**Variables globales que deben configurarse en Airflow:**
- `ENV_MODE`: Define el entorno actual (`dev`, `staging`, o `prod`). **REQUERIDA**.
- `LOGGING_LEVEL`: Nivel de logging (`INFO`, `DEBUG`, `WARNING`, `ERROR`, `CRITICAL`). **REQUERIDA**.

### Notas importantes

- El entorno se determina automáticamente desde `ENV_MODE` (Airflow Variable o variable de entorno), o usa `"dev"` por defecto.
- Si una Connection o Variable no existe, el sistema continúa usando los valores del YAML sin error.
- El campo `extra` de las Connections acepta JSON para parámetros adicionales (ej: `{"api_key": "abc123", "endpoint": "/api"}`).
- Mantén `AIRFLOW_UID=50000` en el `.env` de la raíz del proyecto para evitar problemas de permisos en los volúmenes Docker.
- **No es necesario crear la connection `generic_autin_shared_{env}`**. Los módulos GDE y Dynamic Checklist tienen sus propias connections específicas (`generic_autin_gde_{env}` y `generic_autin_dc_{env}`) que incluyen toda la configuración necesaria.

---

## Notas de operación

- Ejecuta `AIRFLOW_UID=$(id -u)` antes de `docker compose up` si levantas el stack en otra máquina Linux.
- Para usar Docker sin `sudo`, añade tu usuario al grupo `docker` y vuelve a iniciar sesión.
- Los logs en vivo están disponibles con `sudo docker compose logs -f`.
- Si necesitas reconstruir la imagen (por ejemplo, cambiar Chrome/Chromedriver), vuelve a ejecutar `./generar_paquete_offline.sh` y distribuye el nuevo bundle.

---

## Tablas finales por ingesta

- Ver detalle en `docs/TABLAS_FINALES_INGESTAS.md`.

---

## DAGs principales

- `dag_neteco`: ETL NetEco (scraper + transform + load + SP), schedule `0 */3 * * *`.
- `dag_neteco_faltantes_report`: reporte XLSX de faltantes NetEco, manual (schedule `None`).
- `dag_autin_gde`: scraper y carga de GDE, schedule `0 */3 * * *`.
- `dag_autin_checklist`: Dynamic Checklist, schedule `0 2,5,8,11,14,17,20,23 * * *`.
- `dag_recibos_sftp_energia`: recibos de energía SFTP (PD/DA), schedule `0 */3 * * *`.
- `dag_etl_sftp_pago_energia`: pagos de energía SFTP, schedule `0 */3 * * *`.
- `dag_etl_sftp_toa`: reportes TOA SFTP, schedule `0 */3 * * *`.
- `dag_etl_clientes_libres`: clientes libres SFTP, schedule `0 */3 * * *`.
- `dag_etl_base_sitios`: base de sitios (base + bitácora), schedule `0 */3 * * *`.
- `dag_etl_sftp_base_suministros_activos`: base suministros activos, schedule `0 */3 * * *`.
- `dag_etl_webindra`: recibos Indra, schedule `0 */3 * * *`.
- `dag_cargaglobal`: carga manual parametrizada, manual (schedule `None`).
- `dag_healthcheck_config`: healthcheck de variables/connections, manual (schedule `None`).

---

## Stored Procedures (SP) en `db/`

Dónde están:
- Código de SPs: `db/fase 3/ods/funcion/`
- Definición de tablas RAW/ODS: `db/fase 3/raw/` y `db/fase 3/ods/tabla/`

SP ↔ DAG (tabla resumen):

| DAG | SP(s) | Origen (RAW) | Destino (ODS / otros) |
| --- | --- | --- | --- |
| dag_recibos_sftp_energia | ods.sp_cargar_sftp_hm_consumo_suministro_da / ods.sp_cargar_sftp_hm_consumo_suministro_pd | raw.sftp_mm_consumo_suministro_da / raw.sftp_mm_consumo_suministro_pd | ods.sftp_hm_consumo_suministro |
| dag_etl_sftp_pago_energia | ods.sp_cargar_sftp_hm_pago_energia | raw.sftp_mm_pago_energia | ods.sftp_hm_pago_energia (errores en public.error_pago_energia) |
| dag_etl_base_sitios | ods.sp_cargar_fs_hm_base_de_sitios / ods.sp_cargar_fs_hm_bitacora_base_sitios | raw.fs_mm_base_de_sitios / raw.fs_mm_bitacora_base_sitios | ods.fs_hm_base_de_sitios / ods.fs_hm_bitacora_base_sitios |
| dag_etl_sftp_toa | ods.sp_cargar_sftp_hd_toa | raw.sftp_hd_toa | ods.sftp_hd_toa |
| dag_etl_clientes_libres | ods.sp_cargar_sftp_hm_clientes_libres | raw.sftp_mm_clientes_libres | ods.sftp_hm_clientes_libres |
| dag_autin_checklist | ods.sp_cargar_dynamic_checklist_tasks | raw.dynamic_checklist_tasks | Tablas ODS de checklist + validaciones extra |
| dag_autin_gde | ods.sp_cargar_gde_tasks | raw.web_mm_autin_infogeneral | ods.web_hm_autin_infogeneral |
| dag_etl_webindra | ods.sp_cargar_web_hm_indra_energia | raw.web_mm_indra_energia | ods.web_hm_indra_energia |
| dag_etl_sftp_base_suministros_activos | (carga directa) | — | ods.sftp_hm_base_suministros_activos |
| dag_neteco | ods.sp_cargar_web_md_neteco | raw.web_md_neteco | ods.web_hd_neteco / ods.web_hd_neteco_diaria |
