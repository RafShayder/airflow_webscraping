# 🐳 Docker Setup - Scraper Integratel

## 📋 Archivos Creados

### **Dockerfile**
- Imagen basada en Python 3.11-slim
- Instala Google Chrome para Selenium
- Configura el entorno de trabajo
- Instala dependencias de Python

### **docker-compose.yml**
- **PostgreSQL 15** en puerto 5432
- **Aplicación scraper** con dependencias
- **Volúmenes** para datos persistentes
- **Red** interna para comunicación

### **.dockerignore**
- Excluye archivos innecesarios del contexto Docker
- Reduce el tamaño de la imagen

### **sql/init.sql**
- Script de inicialización de la base de datos
- Crea esquema `raw`
- Configura permisos de usuario

## 🚀 Cómo Usar

### **1. Preparar archivo .env**
```bash
# Copiar archivo de ejemplo
cp env.example .env

# Editar con tus credenciales
nano .env
```

### **2. Construir y ejecutar**
```bash
# Construir e iniciar todos los servicios
docker-compose up --build

# Ejecutar en segundo plano
docker-compose up -d --build
```

### **3. Ejecutar scripts específicos**
```bash
# Ejecutar GDE.py
docker-compose exec scraper python GDE.py

# Ejecutar dynamic_checklist.py
docker-compose exec scraper python dynamic_checklist.py

# Acceder a la consola del contenedor
docker-compose exec scraper bash
```

### **4. Acceder a la base de datos**
```bash
# Conectar a PostgreSQL
docker-compose exec postgres psql -U scraper_user -d scraper_db

# O desde tu máquina local
psql -h localhost -p 5432 -U scraper_user -d scraper_db
```

## 📊 Base de Datos

### **Configuración:**
- **Host:** localhost (desde tu máquina) / postgres (desde contenedor)
- **Puerto:** 5432
- **Base de datos:** scraper_db
- **Usuario:** scraper_user
- **Contraseña:** scraper_password

### **Esquema:**
- **raw** - Para datos de ingesta

### **Tablas:**
- Se crean automáticamente desde archivos SQL en `/sql/`

## 🔧 Comandos Útiles

### **Ver logs:**
```bash
# Todos los servicios
docker-compose logs

# Solo scraper
docker-compose logs scraper

# Solo postgres
docker-compose logs postgres
```

### **Detener servicios:**
```bash
# Detener
docker-compose down

# Detener y eliminar volúmenes
docker-compose down -v
```

### **Reconstruir:**
```bash
# Reconstruir solo la aplicación
docker-compose build scraper

# Reconstruir todo
docker-compose build --no-cache
```

## 📁 Estructura de Volúmenes

```
./temp/              -> /app/temp (archivos descargados)
./sql/               -> /docker-entrypoint-initdb.d (scripts SQL)
./.env               -> /app/.env (variables de entorno)
postgres_data        -> /var/lib/postgresql/data (datos de BD)
```

## ⚠️ Notas Importantes

1. **Chrome en Docker:** La imagen incluye Chrome para Selenium
2. **Datos persistentes:** Los datos de PostgreSQL se mantienen en el volumen
3. **Archivos de descarga:** Se guardan en `./temp/`
4. **Variables de entorno:** Usa el archivo `.env` para configuración
5. **Scripts SQL:** Se ejecutan automáticamente al inicializar la BD
6. **Ejecución manual:** El contenedor se mantiene vivo para ejecutar scripts manualmente

## 🐛 Troubleshooting

### **Error de permisos:**
```bash
# Dar permisos al directorio temp
chmod 755 temp/
```

### **Error de conexión a BD:**
```bash
# Verificar que PostgreSQL esté corriendo
docker-compose ps

# Ver logs de PostgreSQL
docker-compose logs postgres
```

### **Error de Chrome:**
```bash
# Reconstruir imagen
docker-compose build --no-cache scraper
```
