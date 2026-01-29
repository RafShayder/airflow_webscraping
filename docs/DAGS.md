# DAGs y Schedules

Este documento lista todos los DAGs disponibles y su configuración.

---

## Lista de DAGs

| DAG | Descripción | Schedule | Frecuencia |
|-----|-------------|----------|------------|
| `DAG_neteco` | ETL NetEco (scraper + transform + load + SP) | `0 5 * * *` | Cada día 5 am |
| `DAG_neteco_alertas` | Reportes XLSX de alertas NetEco (faltantes y anomalías) | `None` | Manual |
| `DAG_gde` | Scraper y carga de datos GDE | `0 3 * * *` | Cada día 3 am |
| `DAG_dynamic_checklist` | ETL completo para Dynamic Checklist | `0 2 1 * *` | Cada 01 de mes 2 am |
| `DAG_sftp_energia` | Recibos de energía SFTP (PD/DA) | `0 4 1 * *` | Cada 01 de mes 4 am |
| `DAG_sftp_pago_energia` | Pagos de energía SFTP | `0 0 * * 1` | Cada lunes medianoche |
| `DAG_sftp_toa` | Reportes TOA SFTP | `0 2 * * *` | Cada día 2 am |
| `DAG_clientes_libres` | Clientes libres SFTP | `0 4 1 * *` | Cada 01 de mes 4 am |
| `DAG_base_sitios` | Base de sitios (base + bitácora) | `0 1 1 * *` | Cada 01 de mes 1 am |
| `DAG_sftp_base_suministros_activos` | Base suministros activos SFTP | `0 4 1 * *` | Cada 01 de mes 4 am |
| `DAG_webindra` | Recibos Indra web | `0 4 1 * *` | Cada 01 de mes 4 am |
| `DAG_cargaglobal` | Carga manual parametrizada | `None` | Manual |
| `DAG_healthcheck_config` | Healthcheck de variables/connections | `None` | Manual |

---

## Horarios de ejecución automática

- **Diarios:** 2 am (TOA), 3 am (GDE), 5 am (NetEco)
- **Semanales:** Lunes medianoche (Pago Energía)
- **Mensuales (día 1):** 1 am (Base Sitios), 2 am (Checklist), 4 am (Clientes Libres, Suministros Activos, WebIndra, Recibos Energía)

---

## Stored Procedures

Los stored procedures están en `db/fase 3/ods/funcion/`. Definición de tablas en `db/fase 3/raw/` y `db/fase 3/ods/tabla/`.

| DAG | SP(s) | Origen (RAW) | Destino (ODS) |
|-----|-------|--------------|---------------|
| DAG_neteco | `ods.sp_cargar_web_md_neteco` | `raw.web_md_neteco` | `ods.web_hd_neteco`, `ods.web_hd_neteco_diaria` |
| DAG_gde | `ods.sp_cargar_web_hm_autin_infogeneral` | `raw.web_mm_autin_infogeneral` | `ods.web_hm_autin_infogeneral` |
| DAG_dynamic_checklist | `ods.sp_validacion_hm_checklist` | 47 tablas raw.* checklist | Validación de duplicados |
| DAG_sftp_energia | `ods.sp_cargar_sftp_hm_consumo_suministro_da/pd` | `raw.sftp_mm_consumo_suministro_da/pd` | `ods.sftp_hm_consumo_suministro` |
| DAG_sftp_pago_energia | `ods.sp_cargar_sftp_hm_pago_energia` | `raw.sftp_mm_pago_energia` | `ods.sftp_hm_pago_energia` |
| DAG_sftp_toa | `ods.sp_cargar_sftp_hd_toa` | `raw.sftp_hd_toa` | `ods.sftp_hd_toa` |
| DAG_clientes_libres | `ods.sp_cargar_sftp_hm_clientes_libres` | `raw.sftp_mm_clientes_libres` | `ods.sftp_hm_clientes_libres` |
| DAG_base_sitios | `ods.sp_cargar_fs_hm_base_de_sitios` | `raw.fs_mm_base_de_sitios` | `ods.fs_hm_base_de_sitios` |
| DAG_webindra | `ods.sp_cargar_web_hm_indra_energia` | `raw.web_mm_indra_energia` | `ods.web_hm_indra_energia` |
| DAG_sftp_base_suministros_activos | (carga directa) | — | `ods.sftp_hm_base_suministros_activos` |

Para más detalle de tablas, ver [TABLAS_FINALES_INGESTAS.md](TABLAS_FINALES_INGESTAS.md).
