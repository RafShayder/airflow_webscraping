CREATE TABLE raw.sol_paneles_solares (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    numero_arreglo text NULL,
    cant_paneles_arreglo text NULL,
    marca text NULL,
    modelo text NULL,
    capacidad_arreglo text NULL,
    voltaje_arreglo text NULL,
    temperatura_arreglo_c text NULL,
    limpieza_paneles text NULL,
    panel_con_rajadura text NULL,
    verificar_conexion_tierra_cajas_combinadoras text NULL,
    inspeccion_arrestores text NULL,
    verificar_operatividad text NULL,
    verificar_estructura_soporte text NULL,
    verificar_ajuste_conexiones_cables_puente text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_sol_paneles_solares_fechacarga_idx 
    ON raw.sol_paneles_solares USING btree (fechacarga);
