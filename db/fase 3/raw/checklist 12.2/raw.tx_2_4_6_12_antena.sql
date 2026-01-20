CREATE TABLE raw.tx_2_4_6_12_antena (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    marca text NULL,
    modelo text NULL,
    banda text NULL,
    diametro text NULL,
    limpieza_antena text NULL,
    sellado_conectores text NULL,
    soporte_latigillo_lanzador text NULL,
    aterramiento text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_tx_2_4_6_12_antena_fechacarga_idx 
    ON raw.tx_2_4_6_12_antena USING btree (fechacarga);

