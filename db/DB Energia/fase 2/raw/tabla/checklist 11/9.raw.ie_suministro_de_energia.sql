

-- ============================================================================
-- 9) IE - SUMINISTRO DE ENERGIA
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.ie_suministro_de_energia (
  task_id                           VARCHAR(255) NOT NULL,
  site_id                           VARCHAR(255) NOT NULL,
  sub_wo_id                         VARCHAR(255) NOT NULL,

  fuente_de_energia                 TEXT,
  compania_electrica                TEXT,
  pertenencia_de_suministro         TEXT,
  tipo_de_tension_del_suministro    TEXT,
  propietario_del_suministro        TEXT,
  num_ro_de_suministro              TEXT,
  num_ro_de_medidor                 TEXT,
  num_ro_de_cliente                 TEXT,
  cantidad_de_fases                 TEXT,
  capacidad_de_itm_de_medidor_a     TEXT,
  calibr_cable_mm2_itm_tab          TEXT,
  calibr_cable_mm2_itm_conces       TEXT,
  voltaje_en_itm_v                  TEXT,
  distan_entre_sumini_estaci_m      TEXT,
  observacion                       TEXT,
  fechacarga                        TIMESTAMP(0) NOT NULL
);
