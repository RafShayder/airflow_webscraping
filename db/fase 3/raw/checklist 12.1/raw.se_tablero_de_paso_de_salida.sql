CREATE TABLE raw.se_tablero_de_paso_de_salida (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    actual_planos_diagra_unifil text NULL,
    manten_preven_tabler_princi text NULL,
    limp_calibr_cambio_analiz text NULL,
    limp_calibr_cambio_transf text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_se_tablero_de_paso_de_salida_fechacarga_idx
ON raw.se_tablero_de_paso_de_salida USING btree (fechacarga);