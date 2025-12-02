CREATE TABLE raw.tx_2_4_6_12_dwdm (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    marca text NULL,
    modelo text NULL,
    nivel_optico_rx text NULL,
    nivel_optico_tx text NULL,
    conectores_transceivers text NULL,
    etiquetado text NULL,
    prueba_redundancia text NULL,
    limpieza_equipo text NULL,
    limpieza_filtro text NULL,
    aterramiento text NULL,
    temperatura text NULL,
    alarmas_externas text NULL,
    backup_configuracion text NULL,
    observacion text NULL,
    ordenamiento_fibras text NULL,
    ventilador text NULL,
    nemonico text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_tx_2_4_6_12_dwdm_fechacarga_idx 
    ON raw.tx_2_4_6_12_dwdm USING btree (fechacarga);
