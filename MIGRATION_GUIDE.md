# 📦 Guía de Migración a Airflow

## 🗂️ Estructura del Proyecto

### **Antes (Desarrollo Local):**
```
scraper-integratel/
├── GDE.py                    # ← Archivos en raíz
├── dynamic_checklist.py
├── ingest_report.py
├── config.py
├── src/
├── sql/
├── Dockerfile                # Docker simple
└── docker-compose.yml
```

### **Ahora (Airflow):**
```
scraper-integratel/
├── 📁 proyectos/                    # ← NUEVA ESTRUCTURA AIRFLOW
│   └── scraper_integratel/         # Código migrado aquí
│       ├── __init__.py
│       ├── GDE.py
│       ├── dynamic_checklist.py
│       ├── ingest_report.py
│       ├── config.py
│       ├── src/
│       ├── sql/
│       └── temp/
│
├── 📁 dags/                         # ← DAGs de Airflow
│   ├── gde_dag.py
│   ├── dynamic_checklist_dag.py
│   └── ingest_dag.py
│
├── 📁 logs/                         # Logs de Airflow
├── 📁 config/                       # Config de Airflow
├── 📁 plugins/                      # Plugins de Airflow
│
├── 🐳 Dockerfile.airflow            # Docker para Airflow
├── 🐳 docker-compose-airflow.yml   # Compose para Airflow
├── 📝 env.airflow.example
└── 📚 README_AIRFLOW.md
```

### **Archivos en Raíz (Legacy - Compatibilidad):**
```
scraper-integratel/
├── GDE.py                    # ← MANTENER para desarrollo local
├── dynamic_checklist.py
├── ingest_report.py
├── config.py
├── src/
├── sql/
├── Dockerfile                # ← MANTENER para Docker simple
└── docker-compose.yml
```

## 🎯 Dos Modos de Operación

### **Modo 1: Desarrollo Local (Archivos Raíz)**
```bash
# Usar archivos de la raíz
python GDE.py
python dynamic_checklist.py

# O con Docker simple
docker-compose up
```

### **Modo 2: Producción con Airflow (Carpeta proyectos/)**
```bash
# Usar configuración Airflow
docker-compose -f docker-compose-airflow.yml up -d

# Acceder a UI: http://localhost:8080
```

## 📋 ¿Qué Archivos Usar?

### **Para Desarrollo Local:**
- ✅ Usar archivos en **raíz**: `GDE.py`, `dynamic_checklist.py`, etc.
- ✅ Usar `Dockerfile` y `docker-compose.yml`
- ✅ Ejecución manual desde terminal

### **Para Producción/Airflow:**
- ✅ Usar archivos en **proyectos/scraper_integratel/**
- ✅ Usar `Dockerfile.airflow` y `docker-compose-airflow.yml`
- ✅ Ejecución automática con scheduling
- ✅ Monitoreo con UI web

## 🔄 Sincronización de Cambios

Si haces cambios en los scripts, debes actualizar **ambas versiones**:

1. **Desarrollo Local** (raíz): Para pruebas rápidas
2. **Airflow** (proyectos/): Para producción

**Recomendación:** Desarrollar en la raíz y luego copiar a proyectos/:
```bash
# Después de probar cambios
cp GDE.py proyectos/scraper_integratel/
cp dynamic_checklist.py proyectos/scraper_integratel/
cp ingest_report.py proyectos/scraper_integratel/
cp config.py proyectos/scraper_integratel/
```

## 🚀 Flujo de Trabajo Recomendado

1. **Desarrollo y Pruebas**: Usar archivos en raíz
   ```bash
   python GDE.py
   ```

2. **Testing con Docker Simple**:
   ```bash
   docker-compose up
   ```

3. **Migrar a Airflow**:
   ```bash
   # Copiar cambios
   cp *.py proyectos/scraper_integratel/
   
   # Desplegar
   docker-compose -f docker-compose-airflow.yml up -d
   ```

4. **Producción**: Solo usar Airflow
   - Scheduling automático
   - Monitoreo
   - Retry logic
   - Logs centralizados

## 📝 Notas Importantes

- 🟢 **Archivos raíz**: Compatible con versión anterior
- 🟢 **Carpeta proyectos/**: Nueva estructura Airflow
- 🟢 **Ambos funcionan**: No hay conflicto
- ⚠️ **Mantener sincronizados**: Cambios en ambos lugares

## 🎯 Decisión Final

### **Opción A: Mantener Ambos (Actual)**
- ✅ Compatibilidad con desarrollo local
- ✅ Migración gradual a Airflow
- ⚠️ Duplicación de código (sincronizar manualmente)

### **Opción B: Solo Airflow (Futuro)**
- ✅ Estructura limpia
- ✅ Un solo lugar para el código
- ❌ Requiere Airflow para desarrollo

**Recomendación**: Mantener Opción A hasta que el equipo esté familiarizado con Airflow.

