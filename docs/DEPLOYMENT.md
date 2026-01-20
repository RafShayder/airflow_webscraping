# Generar paquete offline (Dev)

Este paso se realiza en una máquina de desarrollo con acceso a Internet para crear el bundle que luego se despliega en servidores sin Internet.

---

## Requisitos locales

- Docker Engine 24+ con soporte `buildx`
- Docker Compose v2
- Espacio libre aproximado: 6 GB (imágenes + bundle)
- Archivos binarios presentes en la raíz del repo:
  - `chrome_140_amd64.deb`
  - `chromedriver-linux64.zip`

### Obtener binarios de Chrome y Chromedriver

1. **Descargar Google Chrome 140:**
   ```bash
   curl -O http://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_140.0.7339.185-1_amd64.deb
   ```

2. **Renombrar el archivo:**
   ```bash
   mv google-chrome-stable_140.0.7339.185-1_amd64.deb chrome_140_amd64.deb
   ```

3. **Descargar Chromedriver 140:**
   - Ir a https://googlechromelabs.github.io/chrome-for-testing/
   - Buscar la versión `140.0.7339.185` (o la más cercana compatible)
   - Descargar `chromedriver-linux64.zip` para la plataforma `linux64`

4. **Verificar archivos:**
   ```bash
   ls -la chrome_140_amd64.deb chromedriver-linux64.zip
   ```

> **Nota:** Los nombres de archivo deben ser exactamente `chrome_140_amd64.deb` y `chromedriver-linux64.zip` para que el script de generación los reconozca.

---

## Comando

```bash
./generar_paquete_offline.sh
```

---

## Variables de entorno opcionales

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

| Variable | Descripción | Valor por defecto |
|----------|-------------|-------------------|
| `TARGET_PLATFORM` | Plataforma objetivo | `linux/amd64` |
| `IMAGE_TAG` | Etiqueta de la imagen Docker | `latest` |
| `ARCHIVE_NAME` | Nombre del archivo tar.gz generado | `scraper-integratel-offline.tar.gz` |
| `SKIP_BUILD` | Si es `1`, no ejecuta `docker build` | (vacío) |

---

## Salida típica

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

## Siguiente paso

Una vez generado el paquete, seguir las instrucciones de despliegue en el [README.md](../README.md#despliegue-offline-en-el-servidor-linux-amd64).
