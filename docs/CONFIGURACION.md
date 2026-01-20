# Configuración y Credenciales

Este documento detalla el sistema de configuración de Scraper Integratel.

---

## Prioridad de configuración

El sistema carga configuración desde múltiples fuentes con la siguiente prioridad:

```
Airflow Variables > Airflow Connections > YAML > Variables de entorno
```

---

## Variables Requeridas

| Variable | Descripción | Valores |
|----------|-------------|---------|
| `ENV_MODE` | Entorno actual | `dev`, `staging`, `prod` |
| `LOGGING_LEVEL` | Nivel de logging | `INFO`, `DEBUG`, `WARNING`, `ERROR` |

### Variables adicionales por entorno

En producción, las siguientes variables son **requeridas**:

**PostgreSQL:**
- `POSTGRES_USER_{ENV}`
- `POSTGRES_PASS_PRD` (nota: PROD usa `_PRD`, no `_PROD`)
- `POSTGRES_HOST_{ENV}`
- `POSTGRES_PORT_{ENV}`
- `POSTGRES_DB_{ENV}`

**SFTP DAAS:**
- `SFTP_HOST_DAAS_{ENV}`

---

## Airflow Connections

Las Connections almacenan credenciales. Formato: `{conn_id}_{env}` (ej: `postgres_siom_prod`).

### Connections principales

| Connection ID | Tipo | Descripción |
|---------------|------|-------------|
| `postgres_siom_{env}` | Postgres | Base de datos SIOM |
| `sftp_energia_{env}` | SFTP | Servidor SFTP Energia |
| `sftp_daas_{env}` | SFTP | Servidor SFTP DAAS |
| `sftp_base_sitios_{env}` | SFTP | Base sitios (usa credenciales de sftp_daas) |
| `sftp_base_sitios_bitacora_{env}` | SFTP | Bitácora base sitios (usa credenciales de sftp_daas) |
| `sftp_clientes_libres_{env}` | SFTP | Clientes libres (usa credenciales de sftp_daas) |
| `sftp_toa_{env}` | SFTP | TOA (usa credenciales de sftp_daas) |
| `sftp_pago_energia_{env}` | SFTP | Pago energia (usa credenciales de sftp_energia) |
| `sftp_base_suministros_activos_{env}` | SFTP | Suministros activos (usa credenciales de sftp_energia) |
| `http_webindra_{env}` | HTTP | API Webindra |
| `generic_autin_gde_{env}` | Generic | Credenciales GDE |
| `generic_autin_dc_{env}` | Generic | Credenciales Dynamic Checklist |
| `neteco_{env}` | Generic | Credenciales NetEco |

### Herencia de credenciales SFTP

Algunas connections solo necesitan configurar `extras` (rutas), las credenciales se heredan:

| Connection | Hereda credenciales de |
|------------|------------------------|
| `sftp_base_sitios` | `sftp_daas` |
| `sftp_base_sitios_bitacora` | `sftp_daas` |
| `sftp_clientes_libres` | `sftp_daas` |
| `sftp_toa` | `sftp_daas` |
| `sftp_pago_energia` | `sftp_energia` |
| `sftp_base_suministros_activos` | `sftp_energia` |

---

## Ejemplos de Connections

### PostgreSQL

```
Connection Id: postgres_siom_prod
Connection Type: Postgres
Host: 10.226.17.162
Port: 5425
Schema: siom_prod
Login: usuario
Password: ********
Extra (JSON): {"application_name": "airflow"}
```

### SFTP (con credenciales)

```
Connection Id: sftp_energia_prod
Connection Type: SFTP
Host: 10.252.206.132
Port: 22
Login: usuario_sftp
Password: ********
Extra (JSON): {
  "default_remote_dir": "/ruta/remota",
  "default_local_dir": "/opt/airflow/data"
}
```

### SFTP (hereda credenciales)

```
Connection Id: sftp_base_sitios_prod
Connection Type: SFTP
Host: (vacío - hereda de sftp_daas)
Port: (vacío)
Login: (vacío)
Password: (vacío)
Extra (JSON): {
  "default_remote_dir": "/ruta/base_sitios",
  "default_local_dir": "/opt/airflow/data/base_sitios"
}
```

### HTTP

```
Connection Id: http_webindra_prod
Connection Type: HTTP
Host: https://webindra.example.com
Login: usuario
Password: ********
Extra (JSON): {
  "headers": {"Content-Type": "application/json"},
  "timeout": 30
}
```

### Generic

```
Connection Id: neteco_prod
Connection Type: Generic
Host: https://neteco.example.com
Login: usuario
Password: ********
Extra (JSON): {
  "base_url": "https://neteco.example.com"
}
```

---

## Uso en código

```python
from energiafacilities.core.utils import load_config

config = load_config()  # Detecta env desde ENV_MODE
postgres_config = config.get("postgress", {})
sftp_config = config.get("sftp_energia_c", {})
```

Para documentación completa del framework, ver [proyectos/energiafacilities/README.md](../proyectos/energiafacilities/README.md).
