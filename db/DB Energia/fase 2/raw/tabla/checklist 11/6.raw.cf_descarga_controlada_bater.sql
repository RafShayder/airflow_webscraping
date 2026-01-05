
-- ============================================================================
-- 6) CF - DESCARGA CONTROLADA BATERÍAS
--   (columna corregida: 45_min... -> min45_medir_voltaj_bb_que)
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.cf_descarga_controlada_bater (
    task_id                          VARCHAR(255) NOT NULL,
    site_id                          VARCHAR(255) NOT NULL,
    sub_wo_id                        VARCHAR(255) NOT NULL,

    equipo_conectado_a_baterias      TEXT,
    cod_unico_cuadro_fza_ups_asoc    TEXT,
    nom_cuadro_fza_ups_asoc          TEXT,
    carga_cuadro_fuerza_adc          TEXT,
    marca_de_cuadro_de_fuerza        TEXT,
    porcen_corrie_carga_bat          TEXT,
    capacidad_total_bb_teorica_ah    TEXT,
    cantidad_bancos_bat_asociados    TEXT,

    codigo_de_bat1                   TEXT,
    capacidad_bb1_ah                 TEXT,
    marca_bb1                        TEXT,
    anio_instalacion_bb1             TEXT,

    codigo_de_bat2                   TEXT,
    capacidad_bb2_ah                 TEXT,
    marca_bb2                        TEXT,
    anio_instalacion_bb2             TEXT,

    codigo_de_bat3                   TEXT,
    capacidad_bb3_ah                 TEXT,
    marca_bb3                        TEXT,
    anio_instalacion_bb3             TEXT,

    codigo_de_bat4                   TEXT,
    capacidad_bb4_ah                 TEXT,
    marca_bb4                        TEXT,
    anio_instalacion_bb4             TEXT,

    codigo_de_bat5                   TEXT,
    capacidad_bb5_ah                 TEXT,
    marca_bb5                        TEXT,
    anio_instalacion_bb5             TEXT,

    codigo_de_bat6                   TEXT,
    capacidad_bb6_ah                 TEXT,
    marca_bb6                        TEXT,
    anio_instalacion_bb6             TEXT,

    codigo_de_bat7                   TEXT,
    capacidad_bb7_ah                 TEXT,
    marca_bb7                        TEXT,
    anio_instalacion_bb7             TEXT,

    volt_rectif_a_los_15_min_vdc     TEXT,
    carga_asume_rectif_15_min_adc    TEXT,
    carga_asume_bb1_15_min_adc       TEXT,
    carga_asume_bb2_15_min_adc       TEXT,
    carga_asume_bb3_15_min_adc       TEXT,
    carga_asume_bb4_15_min_adc       TEXT,
    carga_asume_bb5_15_min_adc       TEXT,
    carga_asume_bb6_15_min_adc       TEXT,
    carga_asume_bb7_15_min_adc       TEXT,

    volt_rectif_a_los_30_min_vdc     TEXT,
    carga_asume_rectif_30_min_adc    TEXT,
    carga_asume_bb1_30_min_adc       TEXT,
    carga_asume_bb2_30_min_adc       TEXT,
    carga_asume_bb3_30_min_adc       TEXT,
    carga_asume_bb4_30_min_adc       TEXT,
    carga_asume_bb5_30_min_adc       TEXT,
    carga_asume_bb6_30_min_adc       TEXT,
    carga_asume_bb7_30_min_adc       TEXT,

    volt_rectif_a_los_45_min_vdc     TEXT,
    carga_asume_rectif_45_min_adc    TEXT,
    carga_asume_bb1_45_min_adc       TEXT,
    carga_asume_bb2_45_min_adc       TEXT,
    carga_asume_bb3_45_min_adc       TEXT,
    carga_asume_bb4_45_min_adc       TEXT,
    carga_asume_bb5_45_min_adc       TEXT,
    carga_asume_bb6_45_min_adc       TEXT,
    carga_asume_bb7_45_min_adc       TEXT,

    min45_medir_voltaj_bb_que        TEXT,
    cuadro_fza_tiene_autono          TEXT,
    hay_algun_bb_malas_condic_que    TEXT,
    bb_en_malas_condiciones          TEXT,
    hay_alguna_celda_cuyo_v_debajo   TEXT,
    indica_numero_celdas_malas       TEXT,
    observacion_general              TEXT,
    fechacarga                       TIMESTAMP(0) NOT NULL
);
