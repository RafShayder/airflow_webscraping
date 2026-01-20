

-- ============================================================================
-- 11) IE - TABLERO SECUNDARIO
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.ie_tablero_secundario (
  task_id                          VARCHAR(255) NOT NULL,
  site_id                          VARCHAR(255) NOT NULL,
  sub_wo_id                        VARCHAR(255) NOT NULL,

  nombre_de_tablero                TEXT,
  capacidad_itm_general_a          TEXT,
  voltaje_l1_l2_v                  TEXT,
  voltaje_l2_l3_v                  TEXT,
  voltaje_l3_l1_v                  TEXT,
  corriente_l1_a                   TEXT,
  corriente_l2_a                   TEXT,
  corriente_l3_a                   TEXT,
  corriente_n_a                    TEXT,
  limp_ajuste_estruc_metali        TEXT,
  repintado_de_areas_aisladas      TEXT,
  tipo_de_cable                    TEXT,
  calibre_de_cable                 TEXT,
  aterramiento                     TEXT,
  observacion                      TEXT,
  actual_diagra_unifil             TEXT,
  regist_valor_ajuste_protec       TEXT,
  rotulacion_de_equipos            TEXT,
  tipo_propiedad                   TEXT,
  si_tabler_es_tercer_especi_nom   TEXT,
  fechacarga                       TIMESTAMP(0) NOT NULL
);

