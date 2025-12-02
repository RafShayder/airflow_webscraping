CREATE TABLE raw.radio_6_12_18_bbu (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    marca text NULL,
    modelo text NULL,
    tipo text NULL,
    tecnologia text NULL,
    ajuste_conect_banda_base text NULL,
    estado_aterra_banda_base text NULL,
    ajuste_de_conectores_ovps text NULL,
    estado_conexi_f_banda_base text NULL,
    estado_de_aterramiento_de_rfs text NULL,
    estado_de_cable_categoria_6 text NULL,
    estado_de_fo_banda_base text NULL,
    limpieza_equipo text NULL,
    limpieza_filtro_fan text NULL,
    realizar_backup text NULL,
    realizar_medicion_roe_gestor text NULL,
    observacion text NULL,
    estado_ferreteria_de_la_torre text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_radio_6_12_18_bbu_fechacarga_idx
ON raw.radio_6_12_18_bbu USING btree (fechacarga);
