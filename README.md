# Airflow DAG

Stack de Apache Airflow para ejecutarse en servidores Linux `amd64` sin acceso a Internet.

---

## 📑 Índice

1. [Instalación de Docker CE (Requisito previo)](#instalación-de-docker-ce-requisito-previo)
2. [Despliegue offline en el servidor](#despliegue-offline-en-el-servidor-linux-amd64)
3. [Generar el paquete offline (Dev)](#generar-el-paquete-offline-dev)
4. [Configuración y credenciales](#configuración-y-credenciales)
5. [Notas de operación](#notas-de-operación)

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
  "proxy": "telefonica01.gp.inet:8080",
  "headless": true,
}
```

- `date_mode`: `1` aplica el rango manual definido en `date_from`/`date_to`; `2` calcula el rango automático (por ejemplo, último periodo). Deja el valor predeterminado (`2`) salvo que necesites forzar fechas específicas.
- Las credenciales del portal se cargan desde los campos nativos de la Connection (`Login` y `Password`); no es necesario duplicarlas en `Extras`.
- **Alternativa**: También puedes usar Variables de Airflow con el prefijo `TELEOWS_` (ej: `TELEOWS_PROXY`, `TELEOWS_HEADLESS`) en lugar de la Connection. Los DAGs cargan primero las Variables y luego las sobrescriben con la Connection.

Aun cuando se use la Connection, mantén `AIRFLOW_UID=50000` en el `.env` de la raíz del proyecto para evitar problemas de permisos en los volúmenes Docker.

---

## Notas de operación

- Ejecuta `AIRFLOW_UID=$(id -u)` antes de `docker compose up` si levantas el stack en otra máquina Linux.
- Para usar Docker sin `sudo`, añade tu usuario al grupo `docker` y vuelve a iniciar sesión.
- Los logs en vivo están disponibles con `sudo docker compose logs -f`.
- Si necesitas reconstruir la imagen (por ejemplo, cambiar Chrome/Chromedriver), vuelve a ejecutar `./generar_paquete_offline.sh` y distribuye el nuevo bundle.
