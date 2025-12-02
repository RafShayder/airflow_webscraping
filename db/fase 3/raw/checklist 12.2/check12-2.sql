---1

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
-------------2
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
-----3
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
------4
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
------5
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

---6
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
------7
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
-----8
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
------9

CREATE TABLE raw.tx_2_4_6_12_sdh (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    ventilador text NULL,
    nemonico text NULL,
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
    observacion text NULL,
    backup_configuracion text NULL,
    medicion_e1s_ddf_equipos_alcatel text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_tx_2_4_6_12_sdh_fechacarga_idx 
    ON raw.tx_2_4_6_12_sdh USING btree (fechacarga);
----10
CREATE TABLE raw.tx_bh_2_4_6_12_gwc (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    marca text NULL,
    modelo text NULL,
    conectores_transceivers text NULL,
    etiquetado text NULL,
    prueba_redundancia text NULL,
    limpieza_equipo text NULL,
    limpieza_filtro text NULL,
    temperatura_ventilador text NULL,
    alarmas_externas text NULL,
    observacion text NULL,
    backup_configuracion text NULL,
    nemonico text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_tx_bh_2_4_6_12_gwc_fechacarga_idx 
    ON raw.tx_bh_2_4_6_12_gwc USING btree (fechacarga);
----11
CREATE TABLE raw.tx_bh_2_4_6_12_gwd (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    marca text NULL,
    modelo text NULL,
    conectores_transceivers text NULL,
    etiquetado text NULL,
    prueba_redundancia text NULL,
    limpieza_equipo text NULL,
    limpieza_filtro text NULL,
    temperatura_ventilador text NULL,
    alarmas_externas text NULL,
    observacion text NULL,
    backup_configuracion text NULL,
    nemonico text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_tx_bh_2_4_6_12_gwd_fechacarga_idx 
    ON raw.tx_bh_2_4_6_12_gwd USING btree (fechacarga);
----12

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
