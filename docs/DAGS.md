# DAGs y Schedules

Este documento lista todos los DAGs disponibles y su configuración.

---

## Lista de DAGs

| DAG | Descripción | Schedule | Frecuencia |
|-----|-------------|----------|------------|
| `DAG_neteco` | ETL NetEco (scraper + transform + load + SP) | `0 */3 * * *` | Cada 3 horas |
| `DAG_neteco_alertas` | Reportes XLSX de alertas NetEco (faltantes y anomalías) | `None` | Manual |
| `DAG_gde` | Scraper y carga de datos GDE | `0 */3 * * *` | Cada 3 horas |
| `DAG_dynamic_checklist` | ETL completo para Dynamic Checklist | `0 2,5,8,11,14,17,20,23 * * *` | Cada 3h (desfasado +2h) |
| `DAG_sftp_energia` | Recibos de energía SFTP (PD/DA) | `0 */3 * * *` | Cada 3 horas |
| `DAG_sftp_pago_energia` | Pagos de energía SFTP | `0 */3 * * *` | Cada 3 horas |
| `DAG_sftp_toa` | Reportes TOA SFTP | `0 */3 * * *` | Cada 3 horas |
| `DAG_clientes_libres` | Clientes libres SFTP | `0 */3 * * *` | Cada 3 horas |
| `DAG_base_sitios` | Base de sitios (base + bitácora) | `0 */3 * * *` | Cada 3 horas |
| `DAG_sftp_base_suministros_activos` | Base suministros activos SFTP | `0 */3 * * *` | Cada 3 horas |
| `DAG_webindra` | Recibos Indra web | `0 */3 * * *` | Cada 3 horas |
| `DAG_cargaglobal` | Carga manual parametrizada | `None` | Manual |
| `DAG_healthcheck_config` | Healthcheck de variables/connections | `None` | Manual |

---

## Horarios de ejecución automática

- **Cada 3 horas (estándar):** 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00
- **Cada 3 horas (desfasado):** 02:00, 05:00, 08:00, 11:00, 14:00, 17:00, 20:00, 23:00

---

## Stored Procedures

Los stored procedures están en `db/fase 3/ods/funcion/`. Definición de tablas en `db/fase 3/raw/` y `db/fase 3/ods/tabla/`.

| DAG | SP(s) | Origen (RAW) | Destino (ODS) |
|-----|-------|--------------|---------------|
| DAG_neteco | `ods.sp_cargar_web_md_neteco` | `raw.web_md_neteco` | `ods.web_hd_neteco`, `ods.web_hd_neteco_diaria` |
| DAG_gde | `ods.sp_cargar_gde_tasks` | `raw.web_mm_autin_infogeneral` | `ods.web_hm_autin_infogeneral` |
| DAG_dynamic_checklist | `ods.sp_cargar_dynamic_checklist_tasks` | `raw.dynamic_checklist_tasks` | Tablas ODS checklist |
| DAG_sftp_energia | `ods.sp_cargar_sftp_hm_consumo_suministro_da/pd` | `raw.sftp_mm_consumo_suministro_da/pd` | `ods.sftp_hm_consumo_suministro` |
| DAG_sftp_pago_energia | `ods.sp_cargar_sftp_hm_pago_energia` | `raw.sftp_mm_pago_energia` | `ods.sftp_hm_pago_energia` |
| DAG_sftp_toa | `ods.sp_cargar_sftp_hd_toa` | `raw.sftp_hd_toa` | `ods.sftp_hd_toa` |
| DAG_clientes_libres | `ods.sp_cargar_sftp_hm_clientes_libres` | `raw.sftp_mm_clientes_libres` | `ods.sftp_hm_clientes_libres` |
| DAG_base_sitios | `ods.sp_cargar_fs_hm_base_de_sitios` | `raw.fs_mm_base_de_sitios` | `ods.fs_hm_base_de_sitios` |
| DAG_webindra | `ods.sp_cargar_web_hm_indra_energia` | `raw.web_mm_indra_energia` | `ods.web_hm_indra_energia` |
| DAG_sftp_base_suministros_activos | (carga directa) | — | `ods.sftp_hm_base_suministros_activos` |

Para más detalle de tablas, ver [TABLAS_FINALES_INGESTAS.md](TABLAS_FINALES_INGESTAS.md).
