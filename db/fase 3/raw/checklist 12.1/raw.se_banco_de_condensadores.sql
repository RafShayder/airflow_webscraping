CREATE TABLE raw.se_banco_de_condensadores (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    existe_banco_de_condensadores text NULL,
    operan_banco_conden text NULL,
    capacidad text NULL,
    cantidad_de_pasos text NULL,
    operatividad_de_pasos text NULL,
    limpieza_con_aspiradora text NULL,
    rev_fusibl_bobina_termos text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_se_banco_de_condensadores_fechacarga_idx
ON raw.se_banco_de_condensadores USING btree (fechacarga);
