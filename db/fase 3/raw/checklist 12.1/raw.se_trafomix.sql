CREATE TABLE raw.se_trafomix (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    trafom_conect_aguas_abajo text NULL,
    trafom_perten_telefo_conces text NULL,
    si_solici_corte_conces_nuestr text NULL,
    marca text NULL,
    modelo text NULL,
    capacidad_kva text NULL,
    anio text NULL,
    tipo_de_conversion_amp text NULL,
    relaci_transf_voltaj text NULL,
    sistema text NULL,
    analis_rigide_dielec text NULL,
    limp_ajuste_verif_operat text NULL,
    megado_de_trafomix_mohms text NULL,
    detecccion_fugas_a_tierra text NULL,
    trafom_conect_un_pozo text NULL,
    medici_resist_pozo_media text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_se_trafomix_fechacarga_idx
ON raw.se_trafomix USING btree (fechacarga);
