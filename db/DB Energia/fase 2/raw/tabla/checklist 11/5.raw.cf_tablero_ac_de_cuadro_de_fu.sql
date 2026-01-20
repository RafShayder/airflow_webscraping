

-- ============================================================================
-- 5) CF - TABLERO AC DE CUADRO DE FU
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.cf_tablero_ac_de_cuadro_de_fu (
    task_id                           VARCHAR(255) NOT NULL,
    site_id                           VARCHAR(255) NOT NULL,
    sub_wo_id                         VARCHAR(255) NOT NULL,

    cuadro_fza_que_perten_coloca      TEXT,
    capacidad_de_itm_principal_a      TEXT,
    cantid_itm_no_inclui_interr       TEXT,
    consumo_en_interrupor_generala    TEXT,
    voltaje_en_interrupor_generala    TEXT,
    limp_pintad_tabler_corrie         TEXT,
    limp_ajuste_interr_conexi         TEXT,
    medici_verif_balanc_carga         TEXT,
    limpieza_y_calibracion_de_tvss    TEXT,
    limp_calibr_transf                TEXT,
    limp_calibr_shunt                 TEXT,
    limp_calibr_conexi_tierra         TEXT,
    medici_temper_camara_termog       TEXT,
    actual_diagra_unifil              TEXT,
    rotulacion_de_equipos             TEXT,
    observaciones                     TEXT,
    fechacarga                        TIMESTAMP(0) NOT NULL
);
