

-- ============================================================================
-- 8) IE - MANTENIMIENTO POZO POR POZO
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.ie_mantenimiento_pozo_por_poz (
  task_id                             VARCHAR(255) NOT NULL,
  site_id                             VARCHAR(255) NOT NULL,
  sub_wo_id                           VARCHAR(255) NOT NULL,

  pozo_interv_perten_una_malla        TEXT,
  nnum_pozo_a_intervenir              TEXT,
  nnum_malla_a_intervenir             TEXT,
  sistema_que_protege                 TEXT,
  sala_o_equipos_que_protege          TEXT,
  verif_contin_pozo_otro_pozo         TEXT,
  verif_contin_pozo_barra_equipo      TEXT,
  verif_resist_menor_5_ohmios_bt      TEXT,
  limp_ajuste_conect_engras           TEXT,
  pintado_y_senalizacion              TEXT,
  estado_regist_tapas_concre          TEXT,
  cambio_repara_tapas_concre          TEXT,
  debe_echars_agua_thorge_confir      TEXT,
  debe_echars_agua_thorge_confir_1    TEXT,
  lectur_resist_despue_echar          TEXT,
  observacion                         TEXT,
  fechacarga                          TIMESTAMP(0) NOT NULL
);
