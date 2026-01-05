
-- ============================================================================
-- 2) CF - BASTIDOR DISTRIBUCION
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.cf_bastidor_distribucion (
    task_id                         VARCHAR(255) NOT NULL,
    site_id                         VARCHAR(255) NOT NULL,
    sub_wo_id                       VARCHAR(255) NOT NULL,

    cuadro_fza_que_perten_coloca    TEXT,
    numero_unidad_distri            TEXT,
    limp_ajuste_pintad_bastid       TEXT,
    verif_fusibl_e_interr           TEXT,
    verif_cables_barras_interc      TEXT,
    calculo_de_caida_de_tension     TEXT,
    corriente_de_llegada_a          TEXT,
    voltaje_de_llegada_v            TEXT,
    temper_camara_termog_c          TEXT,
    limp_calibr_medido_consum       TEXT,
    comprobacion_de_alarmas         TEXT,
    actual_diagra_unifil            TEXT,
    rotula_bastid_distri_enumer     TEXT,
    observacion                     TEXT,
    fechacarga                      TIMESTAMP(0) NOT NULL
);