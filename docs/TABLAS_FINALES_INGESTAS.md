# Tablas finales por ingesta

## NetEco (web)
- Tablas destino: `ods.web_hd_neteco`, `ods.web_hd_neteco_diaria`
- Archivo origen: `neteco_diario.zip`

## GDE (Integratel)
- Tabla destino: `ods.web_hm_autin_infogeneral`
- Archivo origen: `Console_GDE_export.xlsx`

## Dynamic Checklist (Integratel)
- Archivo origen: `DynamicChecklist_SubPM.xlsx`

| Formulario | Tabla destino |
| --- | --- |
| CF - BANCO DE BATERIAS | `raw.cf_banco_de_baterias` |
| CF - BASTIDOR DISTRIBUCION | `raw.cf_bastidor_distribucion` |
| CF - CUADRO DE FUERZA | `raw.cf_cuadro_de_fuerza` |
| CF - MODULOS RECTIFICADORES | `raw.cf_modulos_rectificadores` |
| CF - TABLERO AC DE CUADRO DE FU | `raw.cf_tablero_ac_de_cuadro_de_fu` |
| CF_ - DESCARGA CONTROLADA BATER | `raw.cf_descarga_controlada_bater` |
| IE - DATOS SPAT GENERAL | `raw.ie_datos_spat_general` |
| IE - MANTENIMIENTO POZO POR POZ | `raw.ie_mantenimiento_pozo_por_poz` |
| IE - SUMINISTRO DE ENERGIA | `raw.ie_suministro_de_energia` |
| IE - TABLERO PRINCIPAL | `raw.ie_tablero_principal` |
| IE - TABLERO SECUNDARIO | `raw.ie_tablero_secundario` |
| GE - GRUPO ELECTROGENO | `raw.ge_grupo_electrogeno` |
| GE - INFO GENERAL DE TANQUE | `raw.ge_info_general_de_tanque` |
| GE_ - LIMP INTERNA TK | `raw.ge_limp_interna_tk` |
| GE_ - TRANSFERENCIA CON CARGA | `raw.ge_transferencia_con_carga` |
| GE - TABLERO DE TRANSFERENCIA A | `raw.ge_tablero_de_transferencia_a` |
| RADIO_6-12-18 BAS_CF_BB | `raw.radio_6_12_18_bas_cf_bb` |
| RADIO_6-12-18 BBU | `raw.radio_6_12_18_bbu` |
| SE - BANCO DE CONDENSADORES | `raw.se_banco_de_condensadores` |
| SE - PROTECCION Y PARARRAYOS | `raw.se_proteccion_y_pararrayos` |
| SE - TABLERO DE PASO DE SALIDA | `raw.se_tablero_de_paso_de_salida` |
| SE - TRAFOMIX | `raw.se_trafomix` |
| SE - TRANSFORMADOR DE POTENCIA | `raw.se_transformador_de_potencia` |
| SOL - BANCO DE BATERIAS SOLARES | `raw.sol_banco_de_baterias_solares` |
| SOL - CONTROLADOR SOLAR | `raw.sol_controlador_solar` |
| SOL - INFORMACION GENERAL SIST | `raw.sol_informacion_general_sist` |
| SOL - PANELES SOLARES | `raw.sol_paneles_solares` |
| TX-BH_2-4-6-12 GWC | `raw.tx_bh_2_4_6_12_gwc` |
| TX-BH_2-4-6-12 GWD | `raw.tx_bh_2_4_6_12_gwd` |
| TX-BH_2-4-6-12 GWT | `raw.tx_bh_2_4_6_12_gwt` |
| TX_2-4-6-12 ANTENA | `raw.tx_2_4_6_12_antena` |
| TX_2-4-6-12 DWDM | `raw.tx_2_4_6_12_dwdm` |
| TX_2-4-6-12 IDU | `raw.tx_2_4_6_12_idu` |
| TX_2-4-6-12 ODU | `raw.tx_2_4_6_12_odu` |
| TX_2-4-6-12 SDH | `raw.tx_2_4_6_12_sdh` |
| AVR | `raw.avr` |
| CLIMA - CONDENSADOR | `raw.clima_condensador` |
| CLIMA - EVAPORADOR | `raw.clima_evaporador` |
| Clima | `raw.clima` |
| INVERSOR | `raw.inversor` |
| LMT_LBT - CONDUCTORES Y PROTECC | `raw.lmt_lbt_conductores_y_protecc` |
| LMT_LBT - DATOS DE LINEA GENERA | `raw.lmt_lbt_datos_de_linea_genera` |
| LMT_LBT - POSTE DE CADA UNO | `raw.lmt_lbt_poste_de_cada_uno` |
| Mantenimiento Preventivo Dinami | `raw.mantenimiento_preventivo_dinami` |
| Mantenimiento preventivo | `raw.mantenimiento_preventivo` |
| UPS - BATERIA DE UPS | `raw.ups_bateria_de_ups` |
| UPS - UPS | `raw.ups_ups` |

## SFTP Energia (PD/DA)
- Tabla destino: `ods.sftp_hm_consumo_suministro`
- Archivo origen (PD): `reporte-consumo-energia-PD-YYYYMM*.xlsx`
- Archivo origen (DA): `reporte-consumo-energia-DA-YYYYMM*.xlsx`

## SFTP Pago Energia
- Tabla destino: `ods.sftp_hm_pago_energia`
- Errores: `public.error_pago_energia_ultimo_lote`
- Archivos origen: Excel en `/PAGOS_ENERGY_ANALYTICS` (hoja "LIQ. Recibos de Luz")

## SFTP TOA
- Tabla destino: `ods.sftp_hd_toa`
- Archivo origen: `TOA*.xlsx`

## SFTP Base Suministros Activos
- Tabla destino: `ods.sftp_hm_base_suministros_activos`
- Archivo origen: `base_suministros_activos*_<MMYY>*.xlsx`

## SFTP Clientes Libres
- Tabla destino: `ods.sftp_hm_clientes_libres`
- Archivo origen: `F-TELEFONICA-<MMYY>(e).xlsx`

## Base Sitios
- Tablas destino: `ods.fs_hm_base_de_sitios`, `ods.fs_hm_bitacora_base_sitios`
- Archivo origen: `BASE_SITIOS_<YYYYMM>.xlsx`

## WebIndra (web)
- Tabla destino: `ods.web_hm_indra_energia`
- Archivo origen: `recibos_energia_indra.xlsx`

## Carga Global (manual)
- Tabla destino: configurable (por defecto `raw.excel_hm_consumo_energia`, SP opcional)
- Archivo origen: `archivo.xlsx` (manual; por defecto `tmp/global/archivo.xlsx`)
