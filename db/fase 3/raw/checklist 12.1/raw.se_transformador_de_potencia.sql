CREATE TABLE raw.se_transformador_de_potencia (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    sistema_al_que_alimenta text NULL,
    existe_lmt_aerea text NULL,
    nivel_de_tension text NULL,
    marca text NULL,
    codigo_de_equipo text NULL,
    modelo text NULL,
    capacidad_kva text NULL,
    anio text NULL,
    estado text NULL,
    tipo_de_ventilacion text NULL,
    tipo_de_conexion text NULL,
    voltaj_alta_transf_kv text NULL,
    voltaj_baja_transf_kv text NULL,
    relaci_transf_voltaj text NULL,
    medici_resist_aislam_alta_baja text NULL,
    medici_resist_aislam_alta text NULL,
    medici_resist_aislam_baja text NULL,
    analis_rigide_dielec text NULL,
    limp_ajuste_verif_operat text NULL,
    transf_conect_un_pozo text NULL,
    marca_contin_entre_pozo_subest text NULL,
    medici_resist_pozo_media text NULL,
    verifi_que_no_este_resumi text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_se_transformador_de_potencia_fechacarga_idx
ON raw.se_transformador_de_potencia USING btree (fechacarga);
