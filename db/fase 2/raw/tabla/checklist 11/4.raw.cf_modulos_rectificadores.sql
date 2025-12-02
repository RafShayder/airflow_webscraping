

-- ============================================================================
-- 4) CF - MODULOS RECTIFICADORES
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.cf_modulos_rectificadores (
    task_id                        VARCHAR(255) NOT NULL,
    site_id                        VARCHAR(255) NOT NULL,
    sub_wo_id                      VARCHAR(255) NOT NULL,

    cuadro_fza_que_perten_coloca   TEXT,
    posici_modulo_rectif_cf        TEXT,
    marca                          TEXT,
    modelo                         TEXT,
    estado                         TEXT,
    voltaje_de_salida              TEXT,
    corrie_salida_solo_coloca      TEXT,
    contra_lectur_medici           TEXT,
    limp_polvo_soplet_rectif       TEXT,
    observacion                    TEXT,
    fechacarga                     TIMESTAMP(0) NOT NULL
);
