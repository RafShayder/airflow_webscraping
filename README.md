# Scraper

Script de automatización para extraer datos usando Selenium.

## Configuración del Entorno

### 1. Activar el entorno virtual
```bash
source venv/bin/activate
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 3. Configurar variables de entorno
Copia el archivo de ejemplo y edita con tus datos:

```bash
cp env.example .env
nano .env  # o tu editor preferido
```

**⚠️ IMPORTANTE:** Debes configurar las credenciales en el archivo `.env`:

```bash
USERNAME=tu_usuario_real
PASSWORD=tu_password_real

DOWNLOAD_PATH=./downloads
MAX_IFRAME_ATTEMPTS=60
MAX_STATUS_ATTEMPTS=60
DATE_MODE=2  # 1: Fechas manuales | 2: Último mes
DATE_FROM=2025-09-01
DATE_TO=2025-09-10
OPTIONS_TO_SELECT=CM,OPM
GDE_OUTPUT_FILENAME= # opcional
DYNAMIC_CHECKLIST_OUTPUT_FILENAME= # opcional
EXPORT_OVERWRITE_FILES=true  # false genera nombres incrementales
```


## Dependencias

- `selenium==4.15.2`: Para automatización del navegador
- `webdriver-manager==4.0.1`: Para gestión automática de drivers
- `python-dotenv==1.0.0`: Para cargar variables de entorno desde archivo .env
