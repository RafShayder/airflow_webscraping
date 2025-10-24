# 🚀 Scraper Integratel - Configuración con Airflow

## 📁 Estructura del Proyecto

```
scraper-integratel/
├── Dockerfile.airflow              # Dockerfile para Airflow con Chrome/ChromeDriver
├── docker-compose-airflow.yml      # Configuración Docker Compose para Airflow
├── requirements.txt                # Dependencias Python
├── env.airflow.example             # Plantilla de variables de entorno
├── dags/                           # DAGs de Airflow
│   ├── gde_dag.py                 # DAG para scraper GDE
│   ├── dynamic_checklist_dag.py   # DAG para scraper Dynamic Checklist
│   └── ingest_dag.py              # DAG para ingesta a PostgreSQL
├── proyectos/                      # Código del proyecto
│   └── scraper-integratel/
│       ├── GDE.py
│       ├── dynamic_checklist.py
│       ├── ingest_report.py
│       ├── config.py
│       ├── src/
│       └── sql/
├── logs/                           # Logs de Airflow
├── config/                         # Configuración de Airflow
└── plugins/                        # Plugins de Airflow
```

## 🔧 Configuración Inicial

### 1. Archivos de Chrome (Para Entornos Sin Internet)

Descarga los siguientes archivos y colócalos en la raíz del proyecto:

```bash
# Chrome 140 para AMD64
wget https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_140.0.6858.0-1_amd64.deb \
  -O chrome_140_amd64.deb

# ChromeDriver para Linux64
wget https://storage.googleapis.com/chrome-for-testing-public/140.0.6858.0/linux64/chromedriver-linux64.zip
```

### 2. Variables de Entorno

```bash
# Copiar plantilla de configuración
cp env.airflow.example .env

# Editar y configurar credenciales
nano .env
```

**Variables importantes a configurar:**
- `USERNAME`: Usuario de Integratel
- `PASSWORD`: Contraseña de Integratel

### 3. Construir y Levantar Servicios

```bash
# Construir imagen Docker
docker-compose -f docker-compose-airflow.yml build

# Levantar servicios
docker-compose -f docker-compose-airflow.yml up -d
```

## 📊 Servicios de Airflow

Una vez levantados los servicios, estarán disponibles:

- **Airflow Web UI**: http://localhost:8080
  - Usuario: `airflow`
  - Contraseña: `airflow`
  
- **PostgreSQL**: localhost:5432
  - Base de datos: `airflow`
  - Usuario: `airflow`
  - Contraseña: `airflow`

- **Prometheus**: http://localhost:9090
- **StatsD Exporter**: http://localhost:9102

## 🤖 DAGs Disponibles

### 1. GDE Scraper (`gde_scraper`)
- **Schedule**: Diario a las 6:00 AM
- **Función**: Descarga el reporte Console GDE Export
- **Output**: `/opt/airflow/proyectos/scraper-integratel/temp/Console_GDE_export*.xlsx`

### 2. Dynamic Checklist Scraper (`dynamic_checklist_scraper`)
- **Schedule**: Diario a las 7:00 AM
- **Función**: Descarga el reporte Dynamic Checklist - Sub PM Query
- **Output**: `/opt/airflow/proyectos/scraper-integratel/temp/DynamicChecklist*.xlsx`

### 3. Ingest Reports (`ingest_reports`)
- **Schedule**: Diario a las 9:00 AM
- **Función**: Ingesta los archivos descargados a PostgreSQL
- **Tablas destino**: 
  - `raw.gde_reports`
  - `raw.dynamic_checklist_tasks`

## 🔄 Flujo de Trabajo

```
┌─────────────────┐
│   6:00 AM       │
│  GDE Scraper    │──┐
└─────────────────┘  │
                     │
┌─────────────────┐  │     ┌─────────────────┐
│   7:00 AM       │  │     │   9:00 AM       │
│Dynamic Checklist│──┼────▶│  Ingest Data    │
│    Scraper      │  │     │   to PostgreSQL │
└─────────────────┘  │     └─────────────────┘
                     │
```

## 🛠️ Comandos Útiles

### Gestión de Servicios

```bash
# Ver logs de todos los servicios
docker-compose -f docker-compose-airflow.yml logs -f

# Ver logs de un servicio específico
docker-compose -f docker-compose-airflow.yml logs -f airflow-scheduler

# Reiniciar servicios
docker-compose -f docker-compose-airflow.yml restart

# Detener servicios
docker-compose -f docker-compose-airflow.yml down

# Detener y eliminar volúmenes
docker-compose -f docker-compose-airflow.yml down -v
```

### Ejecución Manual

```bash
# Ejecutar DAG manualmente desde la UI de Airflow
# O desde la línea de comandos:

# Entrar al contenedor de Airflow
docker exec -it <container_id> bash

# Ejecutar DAG
airflow dags trigger gde_scraper
airflow dags trigger dynamic_checklist_scraper
airflow dags trigger ingest_reports
```

### Acceso a Archivos

```bash
# Los archivos descargados se encuentran en:
cd proyectos/scraper-integratel/temp/

# Ver archivos descargados
ls -lh proyectos/scraper-integratel/temp/
```

## 🐛 Troubleshooting

### Problema: No se descargan los archivos

**Solución**: Verificar logs del DAG en la UI de Airflow

```bash
docker-compose -f docker-compose-airflow.yml logs -f airflow-worker
```

### Problema: Error de importación en DAG

**Solución**: Verificar que `PYTHONPATH` esté configurado correctamente

```bash
# Entrar al contenedor
docker exec -it <airflow-worker-container> bash

# Verificar PYTHONPATH
echo $PYTHONPATH

# Verificar que exista el directorio proyectos
ls -la /opt/airflow/proyectos/
```

### Problema: Chrome/ChromeDriver no funciona

**Solución**: Verificar que los archivos estén en la raíz del proyecto

```bash
ls -lh chrome_140_amd64.deb chromedriver-linux64.zip
```

## 📈 Monitoreo

### Métricas con Prometheus

Accede a http://localhost:9090 para ver métricas de Airflow:

- Duración de tareas
- Tasa de éxito/fallo
- Uso de recursos

### Logs Centralizados

Todos los logs se almacenan en:
```
logs/
├── dag_id=gde_scraper/
├── dag_id=dynamic_checklist_scraper/
└── dag_id=ingest_reports/
```

## 🔒 Seguridad

### Credenciales

- ⚠️ **Nunca** commitear el archivo `.env` con credenciales reales
- Usar Airflow Connections para gestionar credenciales en producción
- Configurar secrets management (Vault, AWS Secrets Manager, etc.)

### Acceso Web

- Cambiar usuario/contraseña por defecto de Airflow
- Configurar HTTPS en producción
- Implementar autenticación externa (LDAP, OAuth)

## 📚 Recursos Adicionales

- [Documentación Airflow](https://airflow.apache.org/docs/)
- [Selenium Documentation](https://www.selenium.dev/documentation/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## 🤝 Soporte

Para problemas o consultas, contactar al equipo de desarrollo.

