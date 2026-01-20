CREATE TABLE raw.sol_banco_de_baterias_solares (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    codigo_equipo text NULL,
    numero_banco text NULL,
    marca text NULL,
    modelo text NULL,
    capacidad_ah text NULL,
    fecha_instalacion text NULL,
    temperatura_c text NULL,
    baterias_elementos_anexos text NULL,
    estado_bornes_bateria text NULL,
    estado_puentes_bateria text NULL,
    baterias_sin_deformacion text NULL,
    engrase_puentes_bornes text NULL,
    medicion_impedancia text NULL,
    autonomia_hrs text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_sol_banco_de_baterias_solares_fechacarga_idx 
    ON raw.sol_banco_de_baterias_solares USING btree (fechacarga);
