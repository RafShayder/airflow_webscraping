CREATE TABLE raw.tx_bh_2_4_6_12_gwt (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    marca text NULL,
    modelo text NULL,
    etiquetado text NULL,
    conectores_transceivers text NULL,
    prueba_redundancia text NULL,
    limpieza_equipo text NULL,
    limpieza_filtro text NULL,
    limpieza_ventilador_coriant8609 text NULL,
    alarmas_externas text NULL,
    observacion text NULL,
    backup_configuracion text NULL,
    nemonico text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_tx_bh_2_4_6_12_gwt_fechacarga_idx 
    ON raw.tx_bh_2_4_6_12_gwt USING btree (fechacarga);
