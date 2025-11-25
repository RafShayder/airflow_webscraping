

-- ============================================================================
-- 10) IE - TABLERO PRINCIPAL
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.ie_tablero_principal (
  task_id                         VARCHAR(255) NOT NULL,
  site_id                         VARCHAR(255) NOT NULL,
  sub_wo_id                       VARCHAR(255) NOT NULL,

  nombre_de_tablero               TEXT,
  codigo_de_equipo                TEXT,
  cantidad_de_fases               TEXT,
  capacidad_itm_general_a         TEXT,
  voltaje_l1_l2_v                 TEXT,
  voltaje_l2_l3_v                 TEXT,
  voltaje_l3_l1_v                 TEXT,
  corriente_l1_a                  TEXT,
  corriente_l2_a                  TEXT,
  corriente_l3_a                  TEXT,
  corriente_n_a                   TEXT,
  cantidad_de_itm                 TEXT,
  limp_ajuste_tabler_termin       TEXT,
  repintado_de_areas_aisladas     TEXT,
  tipo_de_cable                   TEXT,
  calibre_de_cable                TEXT,
  tablero_aterrado                TEXT,
  medir_contin_entre_cable        TEXT,
  actual_diagra_unifil            TEXT,
  regist_valor_ajuste_protec      TEXT,
  rotulacion_de_equipos           TEXT,
  observacion                     TEXT,
  fechacarga                      TIMESTAMP(0) NOT NULL
);

