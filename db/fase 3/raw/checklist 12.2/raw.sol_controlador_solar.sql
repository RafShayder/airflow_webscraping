CREATE TABLE raw.sol_controlador_solar (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    cantidad_total_controladores_solares text NULL,
    tipo_controlador_solar text NULL,
    marca text NULL,
    modelo text NULL,
    capacidad_w text NULL,
    carga_total_carga_adc text NULL,
    carga_suministrada_paneles_adc text NULL,
    carga_suministrada_baterias_solares_adc text NULL,
    pinzar_carga_otra_fuente_adc text NULL,
    inspeccion_limpieza_unidad_control text NULL,
    calibracion_alarmas_alto_bajo_voltaje text NULL,
    calibracion_desconexion_reconexion text NULL,
    verificar_consumo text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_sol_controlador_solar_fechacarga_idx 
    ON raw.sol_controlador_solar USING btree (fechacarga);
