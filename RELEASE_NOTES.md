# Paquete Offline - v20251104

Paquete completo para despliegue en servidores Linux `amd64` sin acceso a Internet.

## Contenido

- **Imagen Docker**: `scraper-integratel:latest` (Apache Airflow 3.1.0 con Selenium/Chrome)
- **PostgreSQL**: 16
- **Redis**: 7.2-bookworm
- **Prometheus**: Latest
- **StatsD Exporter**: Latest

## Instrucciones de uso

### 1. Copiar el archivo al servidor
```bash
scp scraper-integratel-offline.tar.gz usuario@servidor:/home/usuario/
```

### 2. Importar las imágenes Docker
```bash
ssh usuario@servidor
cd /home/usuario
sudo gunzip -c scraper-integratel-offline.tar.gz | sudo docker load
```

### 3. Verificar las imágenes
```bash
sudo docker images | grep -E 'scraper-integratel|postgres|redis|prom'
```

### 4. Copiar el repositorio
```bash
scp -r scraper-integratel usuario@servidor:/home/usuario/
```

### 5. Configurar variables de entorno
Asegúrate de que `/home/usuario/scraper-integratel/.env` contiene al menos:
```
AIRFLOW_UID=50000
LOG_LEVEL=INFO
```

### 6. Levantar los servicios
```bash
cd /home/usuario/scraper-integratel
sudo docker compose up -d --pull never
sudo docker compose ps
```

### 7. Verificar que Airflow está corriendo
```bash
curl http://localhost:9095/api/v2/version
```

Acceso a la UI: `http://<host>:9095` (usuario/clave por defecto: `airflow` / `airflow`)

## Notas

- Este paquete es para arquitectura `linux/amd64`
- Requiere Docker Engine y Docker Compose instalados en el servidor
- El archivo pesa aproximadamente 1.3GB
