# 🚀 Guía de Despliegue en Servidor de Producción

Esta guía te lleva paso a paso para subir el archivo `.tar.gz` al servidor e instalar la aplicación.

---

## 📋 Prerrequisitos

### En el Servidor:
- ✅ Docker Engine instalado
- ✅ Espacio en disco: mínimo 5 GB libres
- ✅ Docker Compose instalado (opcional, recomendado)

### Verificar Docker en el Servidor:
```bash
ssh usuario@ip-servidor
docker --version
docker-compose --version
```

---

## 📦 Paso 1: Transferir el Archivo al Servidor

### Opción A: Usando SCP (Recomendado)
```bash
# Desde tu máquina de desarrollo
scp scraper-integratel-docker-image.tar.gz usuario@ip-servidor:/home/usuario/
```

**Ejemplo:**
```bash
scp scraper-integratel-docker-image.tar.gz admin@192.168.1.100:/home/admin/
```

### Opción B: Usando SFTP
```bash
sftp usuario@ip-servidor
put scraper-integratel-docker-image.tar.gz /home/usuario/
exit
```

### Opción C: Mediante USB (Si no hay conexión de red)
```bash
# 1. Copiar archivo a USB
cp scraper-integratel-docker-image.tar.gz /media/usb/

# 2. En el servidor, copiar desde USB
cp /media/usb/scraper-integratel-docker-image.tar.gz /home/usuario/
```

---

## 💾 Paso 2: Conectarse al Servidor

```bash
ssh usuario@ip-servidor
```

---

## 🔄 Paso 3: Importar la Imagen Docker

```bash
# Importar la imagen comprimida
gunzip -c scraper-integratel-docker-image.tar.gz | docker load
```

**Tiempo estimado:** 2-5 minutos (depende del servidor)

**Salida esperada:**
```
Loaded image: scraper-integratel:latest
```

### Verificar que se importó correctamente:
```bash
docker images | grep scraper-integratel
```

**Deberías ver:**
```
scraper-integratel   latest   abc123def456   2 hours ago   3.65GB
```

---

## 📁 Paso 4: Copiar Archivos de Configuración

Necesitas copiar los archivos de configuración del repositorio al servidor:

### Archivos necesarios:
- ✅ `docker-compose.yml`
- ✅ `dags/` (todos los DAGs)
- ✅ `proyectos/` (todo el código)
- ✅ `config/` (configuraciones si existen)
- ✅ `.env` (variables de entorno - OPCIONAL pero recomendado)

### Opción A: Clonar el repositorio
```bash
# En el servidor
git clone https://github.com/adraguidev/scraper-teleows.git
cd scraper-teleows
```

### Opción B: Copiar archivos manualmente
```bash
# Desde tu máquina de desarrollo
scp docker-compose.yml usuario@servidor:/home/usuario/scraper-integratel/
scp -r dags/ usuario@servidor:/home/usuario/scraper-integratel/
scp -r proyectos/ usuario@servidor:/home/usuario/scraper-integratel/
```

---

## ⚙️ Paso 5: Configurar Variables de Entorno

```bash
# En el servidor
cd /home/usuario/scraper-integratel

# Crear archivo .env con tus configuraciones
nano .env
```

### Ejemplo de contenido `.env`:
```bash
# Airflow
AIRFLOW_UID=50000

# Base de datos PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=tu_password_seguro
POSTGRES_DB=airflow

# Variables de aplicación
DB_HOST=postgres
DB_USER=airflow
DB_PASSWORD=tu_password_seguro
```

**Guarda y cierra:** `Ctrl+X`, luego `Y`, luego `Enter`

---

## 🚀 Paso 6: Levantar los Contenedores

```bash
# Iniciar todos los servicios
docker-compose up -d
```

**Tiempo estimado:** 1-2 minutos (primera vez)

### Verificar que todos los contenedores están corriendo:
```bash
docker-compose ps
```

**Deberías ver 8 servicios:**
```
NAME                        STATUS              PORTS
airflow-apiserver          Up (healthy)        0.0.0.0:9095->8080/tcp
airflow-scheduler          Up (healthy)
airflow-dag-processor      Up
airflow-worker             Up
airflow-triggerer          Up
postgres                   Up (healthy)
redis                      Up (healthy)
statsd-exporter            Up
```

---

## ✅ Paso 7: Verificar que Todo Funciona

### Ver los logs:
```bash
# Ver logs de todos los servicios
docker-compose logs -f

# O ver logs de un servicio específico
docker-compose logs airflow-scheduler
docker-compose logs airflow-worker
```

### Verificar la interfaz web:
```bash
# La interfaz web está en el puerto 9095
curl http://localhost:9095/api/v2/version
```

### Abrir en navegador (si tienes acceso):
```
http://ip-servidor:9095
```

**Credenciales por defecto:**
- Usuario: `admin`
- Password: `admin` (cambiar en producción)

---

## 🛠️ Comandos Útiles

### Ver estado de los servicios:
```bash
docker-compose ps
```

### Ver logs en tiempo real:
```bash
docker-compose logs -f
```

### Reiniciar un servicio:
```bash
docker-compose restart airflow-scheduler
```

### Detener todos los servicios:
```bash
docker-compose down
```

### Detener y eliminar volúmenes (CUIDADO - borra datos):
```bash
docker-compose down -v
```

### Acceder al contenedor:
```bash
docker-compose exec airflow-scheduler bash
```

### Ver el espacio utilizado:
```bash
docker system df
```

---

## 🔧 Solución de Problemas

### Error: "Cannot connect to Docker daemon"
```bash
# Verificar que Docker está corriendo
sudo systemctl status docker

# Si no está corriendo
sudo systemctl start docker
```

### Error: "No space left on device"
```bash
# Liberar espacio
docker system prune -a

# Ver espacio disponible
df -h
```

### Error: "Permission denied"
```bash
# Agregar usuario al grupo docker
sudo usermod -aG docker $USER

# Cerrar sesión y volver a entrar
exit
ssh usuario@servidor
```

### Los contenedores no inician:
```bash
# Ver logs detallados
docker-compose logs

# Verificar que la imagen está importada
docker images | grep scraper-integratel

# Verificar que el archivo docker-compose.yml existe
ls -la docker-compose.yml
```

---

## 📊 Estructura de Directorios en el Servidor

```
/home/usuario/scraper-integratel/
├── docker-compose.yml          # Configuración de servicios
├── scraper-integratel-docker-image.tar.gz  # Imagen (ya importada)
├── .env                         # Variables de entorno
├── dags/                        # DAGs de Airflow
│   ├── DAG_dynamic_checklist.py
│   ├── DAG_enervision.py
│   ├── DAG_gde.py
│   └── DAG_prueba.py
├── proyectos/                   # Código de los scrapers
│   ├── teleows/
│   └── enervision/
└── logs/                        # Logs de Airflow (se crea automáticamente)
```

---

## ✅ Checklist de Despliegue

- [ ] Docker instalado en el servidor
- [ ] Archivo `.tar.gz` transferido al servidor
- [ ] Imagen importada exitosamente
- [ ] Archivos de configuración copiados
- [ ] Archivo `.env` configurado
- [ ] Todos los contenedores levantados
- [ ] Interfaz web accesible
- [ ] DAGs visibles en Airflow
- [ ] Logs sin errores

---

## 🎯 Resumen de Comandos

```bash
# 1. Transferir archivo
scp scraper-integratel-docker-image.tar.gz usuario@servidor:/ruta/

# 2. Conectarse al servidor
ssh usuario@servidor

# 3. Importar imagen
gunzip -c scraper-integratel-docker-image.tar.gz | docker load

# 4. Copiar archivos de configuración (git clone o scp)

# 5. Configurar .env
nano .env

# 6. Levantar servicios
docker-compose up -d

# 7. Verificar
docker-compose ps
docker-compose logs -f
```

---

**¡Tu aplicación está lista para producción!** 🎉

Para más información, consulta `INSTALACION DESDE IMAGEN.md`

## 🔒 Solución Rápida: Error de Permisos Docker

Si recibes este error:
```
permission denied while trying to connect to the Docker daemon socket
```

### Solución 1: Usar sudo (Recomendado para primera importación)
```bash
sudo gunzip -c scraper-integratel-docker-image.tar.gz | sudo docker load
```

### Solución 2: Agregar usuario al grupo docker (Solución permanente)
```bash
# Agregar usuario al grupo docker
sudo usermod -aG docker $USER

# O si estás en el usuario daasusr
sudo usermod -aG docker daasusr

# Aplicar cambios sin cerrar sesión (alternativa)
newgrp docker

# Cerrar sesión y volver a entrar
exit
ssh daasusr@lnxsrdvmo0011
```

### Solución 3: Ejecutar como root (No recomendado en producción)
```bash
sudo su -
gunzip -c scraper-integratel-docker-image.tar.gz | docker load
exit
```

### Verificar que funcionó:
```bash
docker images | grep scraper-integratel
```

---

## 📝 Nota Importante

Después de agregar el usuario al grupo docker, **debes cerrar la sesión SSH y reconectarte** para que los cambios tengan efecto.

```bash
# Salir
exit

# Reconectar
ssh daasusr@lnxsrdvmo0011

# Ahora probar sin sudo
docker images
```

