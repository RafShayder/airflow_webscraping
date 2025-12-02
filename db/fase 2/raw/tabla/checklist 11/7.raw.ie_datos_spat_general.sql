
-- ============================================================================
-- 7) IE - DATOS SPAT GENERAL
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.ie_datos_spat_general (
  task_id                        VARCHAR(255) NOT NULL,
  site_id                        VARCHAR(255) NOT NULL,
  sub_wo_id                      VARCHAR(255) NOT NULL,

  cantidad_de_pozos_total        TEXT,
  cantidad_de_mallas             TEXT,
  cantidad_de_pozos_unicos       TEXT,
  existe_spat_completo_para_mt   TEXT,
  pozos_varill_cables_barra      TEXT,
  report_robo_algun_compon       TEXT,
  mencionar_componentes_robados  TEXT,
  fechacarga                     TIMESTAMP(0) NOT NULL
);
