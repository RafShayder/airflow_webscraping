CREATE TABLE raw.tx_2_4_6_12_odu (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    marca text NULL,
    modelo text NULL,
    frecuencia text NULL,
    sub_banda text NULL,
    limpieza_odu text NULL,
    sellado_conectores text NULL,
    aterramiento text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_tx_2_4_6_12_odu_fechacarga_idx 
    ON raw.tx_2_4_6_12_odu USING btree (fechacarga);
