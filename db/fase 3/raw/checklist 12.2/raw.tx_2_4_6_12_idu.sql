CREATE TABLE raw.tx_2_4_6_12_idu (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    ventilador text NULL,
    nemonico text NULL,
    marca text NULL,
    modelo text NULL,
    banda text NULL,
    frecuencia text NULL,
    numero_serie text NULL,
    agc_valor_numerico text NULL,
    conectores text NULL,
    etiquetado text NULL,
    limpieza_equipo text NULL,
    limpieza_filtro text NULL,
    temperatura text NULL,
    alarmas_externas text NULL,
    aterramiento text NULL,
    observacion text NULL,
    backup_configuracion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_tx_2_4_6_12_idu_fechacarga_idx 
    ON raw.tx_2_4_6_12_idu USING btree (fechacarga);
