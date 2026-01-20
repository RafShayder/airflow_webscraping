# sync_sftp.py

Script para sincronizar archivos locales (`dags/` y `proyectos/`) con uno o más servidores SFTP. Respeta `.gitignore` y opera en modo mirror (elimina archivos huérfanos en destino).

---

## Requisitos

```bash
pip install paramiko
```

---

## Configuración

Crear archivo `.env.sftp` en la raíz del proyecto:

### Un solo servidor

```bash
SFTP_HOST=10.226.17.100
SFTP_PORT=22
SFTP_USER=usuario
SFTP_PASS=contraseña
SFTP_REMOTE_PATH=/daas1/analytics
```

### Múltiples servidores

```bash
SFTP_SERVERS=prod,staging

SFTP_HOST_prod=10.226.17.100
SFTP_PORT_prod=22
SFTP_USER_prod=user_prod
SFTP_PASS_prod=pass_prod
SFTP_REMOTE_PATH_prod=/daas1/analytics

SFTP_HOST_staging=10.226.17.101
SFTP_PORT_staging=22
SFTP_USER_staging=user_staging
SFTP_PASS_staging=pass_staging
SFTP_REMOTE_PATH_staging=/daas1/analytics-staging
```

---

## Uso

```bash
# Ver qué se sincronizaría (sin ejecutar cambios)
python sync_sftp.py --dry-run

# Sincronizar a todos los servidores
python sync_sftp.py

# Sincronizar solo a un servidor específico
python sync_sftp.py --server prod

# Sincronizar sin eliminar archivos huérfanos
python sync_sftp.py --no-delete

# Modo verbose (más detalle)
python sync_sftp.py --verbose
```

---

## Opciones

| Flag | Descripción |
|------|-------------|
| `--dry-run`, `-n` | Mostrar qué se haría sin ejecutar cambios |
| `--no-delete` | No eliminar archivos huérfanos en destino |
| `--server`, `-s` | Sincronizar solo a un servidor específico |
| `--verbose`, `-v` | Mostrar más detalles |
| `--env-file` | Usar archivo de configuración alternativo |
