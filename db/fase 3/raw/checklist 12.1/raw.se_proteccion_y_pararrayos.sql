CREATE TABLE raw.se_proteccion_y_pararrayos (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    local_cuenta_con_pararrayos text NULL,
    hay_algun_pararr_averia_descar text NULL,
    pararr_conect_transf text NULL,
    pararrayos_conectado_a_spat text NULL,
    compro_amarre_pararr text NULL,
    limp_oxidac_mastil_pararr text NULL,
    medir_contin_entre_transf text NULL,
    medici_pozo_tierra_conect text NULL,
    manten_seccio_e_interr_intern text NULL,
    manten_tvss_fusibl text NULL,
    ajuste_protec_rele_fusibl text NULL,
    manten_borner_platin_tierra text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_se_proteccion_y_pararrayos_fechacarga_idx
ON raw.se_proteccion_y_pararrayos USING btree (fechacarga);
