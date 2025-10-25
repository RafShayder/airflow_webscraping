# 📦 Instalación de Docker SIN Internet

Esta guía te muestra cómo crear una imagen Docker completamente autocontenida y desplegarla en servidores de producción **SIN necesidad de conexión a internet**.

## 🎯 Proceso Resumido

```
1. Desarrollo (CON internet)
   ↓
2. Crear imagen Docker (incluye TODO)
   ↓
3. Exportar imagen a archivo .tar.gz
   ↓
4. Copiar archivo al servidor de producción
   ↓
5. Importar imagen en el servidor (SIN internet)
   ↓
6. ¡Listo! Ejecutar aplicación
```

## 📝 Paso a Paso

### 1️⃣ Crear la Imagen Docker

```bash
# Construir la imagen (en tu máquina de desarrollo)
docker build -t scraper-integratel:latest -f Dockerfile .
```

Esto crea una imagen que incluye:
- ✅ El código de tu aplicación
- ✅ Python y todas las librerías
- ✅ Dependencias del sistema
- ✅ Configuraciones necesarias

### 2️⃣ Exportar la Imagen a un Archivo

```bash
# Exportar con compresión (archivo más pequeño)
docker save scraper-integratel:latest | gzip > scraper-integratel.tar.gz
```

**Resultado:** Un archivo `.tar.gz` que contiene TODO lo necesario (1-3GB aproximadamente)

### 3️⃣ Copiar al Servidor de Producción

#### Opción A: Con SCP
```bash
scp scraper-integratel.tar.gz usuario@192.168.1.100:/home/usuario/
```

#### Opción B: Con USB
- Copiar archivo a USB
- Conectar USB al servidor
- Copiar desde USB al servidor

### 4️⃣ Importar en el Servidor (SIN Internet)

```bash
# Conectarse al servidor
ssh usuario@192.168.1.100

# Importar la imagen
gunzip -c scraper-integratel.tar.gz | docker load
```

**¡Listo!** La imagen ahora está disponible en tu servidor sin necesidad de internet.

### 5️⃣ Verificar y Ejecutar

```bash
# Verificar que la imagen existe
docker images | grep scraper-integratel

# Ejecutar con docker-compose
docker-compose up -d
```

## 🚀 Comandos Rápidos de Referencia

```bash
# CONSTRUIR imagen
docker build -t scraper-integratel:latest .

# EXPORTAR imagen
docker save scraper-integratel:latest | gzip > image.tar.gz

# TRANSFERIR a servidor
scp image.tar.gz usuario@servidor:/ruta/

# IMPORTAR en servidor (sin internet)
gunzip -c image.tar.gz | docker load

# VERIFICAR
docker images
docker inspect scraper-integratel:latest
```

## ⚠️ Consideraciones Importantes

- **Tamaño del archivo**: 1-3GB (depende de dependencias)
- **Tiempo de transferencia**: Considera el ancho de banda
- **Espacio en disco**: Asegúrate de tener suficiente espacio
- **Versionado**: Siempre etiqueta tus imágenes con versión

## 🔧 Script Automatizado

Ejecuta el script incluido para automatizar todo:

```bash
chmod +x crear_imagen_docker.sh
./crear_imagen_docker.sh full
```

Este script hace todo automáticamente y te da las instrucciones.

## ❓ Preguntas Frecuentes

**P: ¿Necesito internet en el servidor de producción?**  
R: NO. Una vez que importas la imagen, todo funciona offline.

**P: ¿Qué pasa si actualizo el código?**  
R: Repites el proceso (crear nueva imagen, transferir, importar).

**P: ¿Puedo usar diferentes versiones?**  
R: Sí, usa tags diferentes: `scraper-integratel:v1.0`, `scraper-integratel:v2.0`, etc.

## ✅ Ventajas

✅ Funciona SIN internet en producción  
✅ Todas las dependencias incluidas  
✅ Reproducible en cualquier servidor  
✅ Aislado del sistema host  
✅ Fácil de respaldar y restaurar  

---

**¡Listo para producción offline!** 🎉

---

## 🎯 ¿Qué Incluye la Imagen Docker?

La imagen Docker es **completamente autocontenida**. NO necesitas instalar nada adicional en el servidor:

### ✅ INCLUIDO en la imagen:
- ✅ **Python 3.12** (ya viene en la imagen base de Airflow)
- ✅ **Apache Airflow 3.1.0** (sistema de orquestación)
- ✅ **Google Chrome** (para web scraping)
- ✅ **ChromeDriver** (controlador de Chrome)
- ✅ **Todas las librerías Python** (pandas, selenium, etc.)
- ✅ **Todas las dependencias del sistema** (bibliotecas de Chrome)
- ✅ **Tu código de la aplicación**
- ✅ **Configuraciones y variables de entorno**

### ❌ NO necesitas instalar en el servidor:
- ❌ Python
- ❌ pip
- ❌ Librerías Python
- ❌ Google Chrome
- ❌ ChromeDriver
- ❌ Dependencias del sistema
- ❌ Configuraciones manuales

## 📊 Comparación: Con vs Sin Docker

### Método ANTIGUO (sin Docker):
```bash
# Servidor de producción
sudo apt-get update
sudo apt-get install python3.12 python3-pip
pip install pandas selenium numpy ...
pip install apache-airflow
# Descargar Chrome
# Instalar ChromeDriver
# Configurar variables de entorno
# Debuggear conflictos de versiones
# ... horas de configuración ...
```

### Método MODERNO (con Docker): ✅
```bash
# Servidor de producción
docker load < scraper.tar.gz
docker-compose up -d

# ¡Listo! Todo funciona en 2 comandos
```

## 🎁 Ventajas Clave

1. **✅ Cero instalaciones** - La imagen contiene TODO
2. **✅ Mismo resultado siempre** - Reproducible en cualquier servidor  
3. **✅ Aislado** - No afecta el sistema del servidor
4. **✅ Portátil** - Funciona igual en desarrollo, staging y producción
5. **✅ Fácil actualización** - Solo necesitas una nueva imagen

---

**¿El servidor necesita Python?** NO ❌  
**¿El servidor necesita Chrome?** NO ❌  
**¿El servidor necesita librerías?** NO ❌  

**El servidor solo necesita:** Docker Engine ✅

