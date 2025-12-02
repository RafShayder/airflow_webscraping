CREATE TABLE raw.sol_informacion_general_sist (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    tipo_sistema_solar text NULL,
    sistema_solar_operativo_averiado text NULL,
    cantidad_arreglos_paneles_solares text NULL,
    cantidad_total_paneles_solares text NULL,
    capacidad_total_paneles_solares_wp text NULL,
    cantidad_total_banco_baterias text NULL,
    capacidad_total_banco_baterias_ah text NULL,
    cantidad_controladores_solar text NULL,
    verificar_tablero_principal text NULL,
    verificar_cableado_unidad_control text NULL,
    verificar_conexion_tierra text NULL,
    verificacion_ajustes_cables_baterias_sonda_temp text NULL,
    verificar_bajada_cables_paneles_regulador text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_sol_informacion_general_sist_fechacarga_idx 
    ON raw.sol_informacion_general_sist USING btree (fechacarga);
