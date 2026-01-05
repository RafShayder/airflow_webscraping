
CREATE TABLE ods.web_hd_neteco_mensual (
	site_name text NOT NULL,
	manage_object text NOT NULL,
	subnet text NULL,
	mes date NOT NULL,
	energy_consumption_per_month_kwh numeric(18, 4) NULL,
	supply_duration_per_month_h numeric(18, 4) NULL,
	total_energy_consumption_per_month_kwh numeric(18, 4) NULL,
	creation_user varchar(100) DEFAULT (((COALESCE(inet_client_addr()::text, 'local'::text) || '-'::text) || CURRENT_USER::text)) NULL,
	creation_date timestamp(0) DEFAULT clock_timestamp()::timestamp(0) without time zone NULL,
	creation_ip inet DEFAULT inet_client_addr() NULL,
	CONSTRAINT chk_web_hd_neteco_mensual_mes_first_day CHECK ((EXTRACT(day FROM mes) = (1)::numeric)),
	CONSTRAINT pk_web_hd_neteco_mensual PRIMARY KEY (site_name, manage_object, mes)
);
--------
CREATE TABLE ods.web_hd_neteco_diaria (
	site_name text NOT NULL,
	manage_object text NOT NULL,
	subnet text NULL,
	fecha date NOT NULL,
	energy_consumption_per_day_kwh numeric(18, 4) NULL,
	supply_duration_per_day_h numeric(18, 4) NULL,
	total_energy_consumption_per_day_kwh numeric(18, 4) NULL,
	creation_user varchar(100) DEFAULT (((COALESCE(inet_client_addr()::text, 'local'::text) || '-'::text) || CURRENT_USER::text)) NULL,
	creation_date timestamp(0) DEFAULT clock_timestamp()::timestamp(0) without time zone NULL,
	creation_ip inet DEFAULT inet_client_addr() NULL,
	CONSTRAINT pk_web_hd_neteco_diaria PRIMARY KEY (site_name, manage_object, fecha)
);
--------
CREATE TABLE ods.sftp_hm_base_suministros_activos (
    cod_unico_ing text NULL,
    id_sum text NULL,
    suministro_actual text NOT NULL,
    servicios text NULL,
    tarifa text NULL,
    "local" text NULL,
    direccion text NULL,
    distribuidor text NULL,
    proveedor text NULL,
    sistema_electrico text NULL,
    alimentador text NULL,
    tipo_conexion text NULL,
    potencia_contratada text NULL,
    estado_suministro text NULL,
    departamento text NULL,
    provincia text NULL,
    distrito text NULL,
    tipo_distribuidor text NULL,
    motivo_desactivacion text NULL,
    grupo_serv text NULL,
    region text NULL,
    latitud text NULL,
    longitud text NULL,
    ceco text NULL,
    tipo_tercero text NULL,
    flm_actual text NULL,
    grupo_debito text NULL,
    ipt text NULL,

    creation_user varchar(100) DEFAULT (
        (COALESCE(inet_client_addr()::text, 'local'::text) || '-'::text) || CURRENT_USER::text
    ) NULL,
    creation_date timestamp(0) DEFAULT clock_timestamp()::timestamp(0) without time zone NULL,
    creation_ip inet DEFAULT inet_client_addr() NULL,

    CONSTRAINT sftp_hm_base_suministros_activos_pk PRIMARY KEY (suministro_actual)
);

-------
CREATE TABLE raw.web_md_neteco (
	site_name text NULL,
	subnet text NULL,
	manage_object text NULL,
	start_time text NULL,
	energy_consumption_per_hour_kwh text NULL,
	supply_duration_per_hour_h text NULL,
	total_energy_consumption_kwh text NULL
);

--------


CREATE TABLE raw.ge_grupo_electrogeno (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    codigo_unico_de_ge text NULL,
    marca_de_grupo_electrogeno text NULL,
    modelo_de_grupo_electrogeno text NULL,
    serie_de_grupo_electrogeno text NULL,
    potenc_grupo_electr_kva text NULL,
    fases text NULL,
    voltaje_de_grupo_electrogeno_v text NULL,
    ge_rotula_cod_unico_caso_que text NULL,
    marca_motor text NULL,
    modelo_motor text NULL,
    serie_motor text NULL,
    capacidad_hp_motor text NULL,
    fecha_ultimo_cambio_filtro text NULL,
    fecha_ultimo_cambio_filtro_aceite_diesel_refrig text NULL,
    fecha_ultimo_cambio_refrig text NULL,
    ultimo_cambio_filtro_aceite_ha text NULL,
    ultimo_cambio_filtro_petrol_ha text NULL,
    ultimo_cambio_filtro_refrig_ha text NULL,
    ultimo_cambio_filtro_aire_ha text NULL,
    horometro text NULL,
    limpieza_externa text NULL,
    verifi_operat_sujeta_restri text NULL,
    verifi_nivel_refrig_radiad text NULL,
    fecha_de_cambio_de_faja text NULL,
    limp_pintad_ventil text NULL,
    verifi_estado_tinas_aletas text NULL,
    limpia_obstru_sondeo_radiad text NULL,
    verifi_estado_termos text NULL,
    verif_correc_fugas_combus text NULL,
    medir_nivel_aceite_carter text NULL,
    presion_aceite_motor text NULL,
    verifi_func_arranc_ajuste text NULL,
    verifi_func_actuad_bomba text NULL,
    verifi_func_gobern_otros text NULL,
    verifi_func_altern_carga text NULL,
    temperatura_del_motor text NULL,
    resumi_aceite_alguna_parte text NULL,
    observacion text NULL,
    marca_de_generador text NULL,
    modelo_de_generador text NULL,
    potencia_kw_de_generador text NULL,
    limpieza_general text NULL,
    verifi_circul_aire_fresco text NULL,
    verif_ruidos_anorma_vibrac text NULL,
    verifi_operat_resist_deshum text NULL,
    verificar_rejillas_protectoras text NULL,
    verifi_que_radiad_no_tenga text NULL,
    ge_tiene_partes_corroidas text NULL,
    tiene_tanque_externo text NULL,
    observacion_del_ge text NULL,
    cod_grupo_electr_asoc_bat text NULL,
    cantid_bat_arranq_conect_ge text NULL,
    marca text NULL,
    capacidad_ah text NULL,
    cantidad_de_placas text NULL,
    fecha_ultimo_cambio_anotad_bat1 text NULL,
    fecha_ultimo_cambio_anotad_bat2 text NULL,
    fecha_ultimo_cambio_anotad_bat3 text NULL,
    fecha_ultimo_cambio_anotad_bat4 text NULL,
    voltaje_bat1_vdc text NULL,
    voltaje_bat2_vdc text NULL,
    voltaj_bat3_vdc_caso_si_existi text NULL,
    voltaj_bat4_vdc_caso_si_existi text NULL,
    verifi_valore_carga_bat text NULL,
    limp_ajuste_cables_bornes_bat text NULL,
    carga_de_bat_arranqueamp text NULL,
    ge_etiquetado_cod_unico text NULL,
    observacion1 text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_ge_grupo_electrogeno_fechacarga_idx
ON raw.ge_grupo_electrogeno USING btree (fechacarga);
----------------------1
CREATE TABLE raw.ge_info_general_de_tanque (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    codigo_unico_de_tanque text NULL,
    tipo_de_tanque text NULL,
    capacidad_gln text NULL,
    nivel_de_combustible_gln text NULL,
    autono_combus_actual_hrs text NULL,
    hay_prefil_combus_tanque text NULL,
    fecha_ultimo_cambio_prefil text NULL,
    cambiar_pre_filtro text NULL,
    limpia_lijar_pintar_corros text NULL,
    tanque_etiquetado_cod_unico text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_ge_info_general_de_tanque_fechacarga_idx
ON raw.ge_info_general_de_tanque USING btree (fechacarga);
-----------------------2
CREATE TABLE raw.ge_limp_interna_tk (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    tipo_de_tanque text NULL,
    tipo_de_combustible text NULL,
    capacidad_tanque_gal text NULL,
    nivel_actual_tanque_gal text NULL,
    consum_grupo_electr_gal_hora text NULL,
    codigo_unico_tanque text NULL,
    estado_del_tanque text NULL,
    forma_de_tanque text NULL,
    ancho_tanque_rectangular_cm text NULL,
    largo_tanque_rectangular_cm text NULL,
    altura_tanque_rectan_refere_es text NULL,
    diametro_tanque_cilindrico_cm text NULL,
    largo_tanque_cilindrico_cm text NULL,
    estado_de_las_tuberias text NULL,
    tiene_prefil_combus_tanque text NULL,
    fecha_ultimo_cambio_prefil text NULL,
    tipo_de_bomba text NULL,
    tiene_poza_antiderrame text NULL,
    tiene_sensor_de_nivel_o_boya text NULL,
    nivel_inicia_combus_gal text NULL,
    consum_grupo_electr_gal_hora_1 text NULL,
    autono_nivel_inicia_hrs text NULL,
    nivel_final_de_combustible_gal text NULL,
    autonomia_con_nivel_final_hrs text NULL,
    prueba_alarma_bajo_nivel text NULL,
    observaciones text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_ge_limp_interna_tk_fechacarga_idx
ON raw.ge_limp_interna_tk USING btree (fechacarga);
-------------------------3
CREATE TABLE raw.ge_tablero_de_transferencia_a (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    codigo_unico_de_tta text NULL,
    tipo_de_tablero text NULL,
    marca_de_tta text NULL,
    modelo_de_tta text NULL,
    serie_de_tta text NULL,
    capacidad_de_tta_amp text NULL,
    operacion_de_tta text NULL,
    estado_de_tta text NULL,
    tta_tiene_controlador text NULL,
    marca_de_controlador text NULL,
    modelo_de_controlador text NULL,
    tta_esta_rotulado text NULL,
    voltaje_vrs_v text NULL,
    voltaje_vst_v text NULL,
    voltaje_vrt_v text NULL,
    voltaje_vrn_v text NULL,
    voltaje_vsn_v text NULL,
    voltaje_vtn_v text NULL,
    intensidad_ir_a text NULL,
    intensidad_is_a text NULL,
    intensidad_it_a text NULL,
    intensneutro_in_a text NULL,
    frecuencia_hz text NULL,
    tta_etiquetado_cod_unico text NULL,
    tempor_partid_tranfe_parada text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_ge_tablero_de_transferencia_a_fechacarga_idx
ON raw.ge_tablero_de_transferencia_a USING btree (fechacarga);
---------------4
CREATE TABLE raw.ge_transferencia_con_carga (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    codigo_de_grupo_electrogeno text NULL,
    codigo_de_tta text NULL,
    ge_esta_en_automatico_o_manual text NULL,
    ge_cuenta_con_controlador text NULL,
    tta_cuenta_con_controlador text NULL,
    tta_averiado text NULL,
    tiene_bat_fecha_vigent text NULL,
    fecha_de_ultimo_cambio_de_bat text NULL,
    revisa_nivel_calida_aceite text NULL,
    se_confir_que_bat_respal text NULL,
    se_reviso_itm_bat_conex_batier text NULL,
    trifasico_o_monofasico text NULL,
    frecuencia_hz text NULL,
    voltaje_r_s_red_comercial_v text NULL,
    voltaje_s_t_red_comercial_v text NULL,
    voltaje_t_r_red_comercial_v text NULL,
    ir_red_comercial_a text NULL,
    is_red_comercial_a text NULL,
    it_red_comercial_a text NULL,
    observacion text NULL,
    se_bajo_itm_ac text NULL,
    tiempo_desde_corte_energi text NULL,
    tiempo_desde_encend_ge_hasta text NULL,
    frecuencia_con_ge text NULL,
    voltaje_r_s_en_ge_v text NULL,
    voltaje_s_t_en_ge_v text NULL,
    voltaje_t_r_en_ge_v text NULL,
    ir_de_ge_a text NULL,
    is_de_ge_a text NULL,
    it_de_ge_a text NULL,
    alarma_corte_red_sale_noc text NULL,
    etique_alarma_que_sale_noc text NULL,
    alarma_sale_localm_cuadro_fza text NULL,
    alarma_ge_operan_sale_noc text NULL,
    etique_alarma_que_sale_noc_1 text NULL,
    alarma_sale_localm_cuadro_fza_1 text NULL,
    tiempo_desde_subida_itm_hasta text NULL,
    tiempo_desde_encend_ge_hasta_2 text NULL,
    observacion_prueba text NULL,
    ge_entro_sin_problemas text NULL,
    algun_parametro_fuera_de_rango text NULL,
    conclusiones_generales text NULL,
    foto_ge_tta_se_vea_que_grupo text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_ge_transferencia_con_carga_fechacarga_idx
ON raw.ge_transferencia_con_carga USING btree (fechacarga);
---------------------------------5
CREATE TABLE raw.radio_6_12_18_bas_cf_bb (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    marca text NULL,
    limp_filtro_gabine text NULL,
    temperatura text NULL,
    verifi_alarma_puerta_abiert text NULL,
    verificacion_sellado_gabinete text NULL,
    observacion text NULL,
    sistema_al_que_alimenta text NULL,
    marca1 text NULL,
    modelo text NULL,
    capacidad_itm_de_banda_base text NULL,
    verificar_etiquetado_de_itm text NULL,
    cantidad_de_rectificadores text NULL,
    ajuste_de_conectores_de_itm text NULL,
    capacidad_amp text NULL,
    carga_actual_de_tecnologia_3g text NULL,
    carga_actual_de_tecnologia_4g text NULL,
    carga_actual_de_rrus_3g text NULL,
    carga_actual_de_rrus_4g text NULL,
    limpieza_y_ajustes_de_modulos text NULL,
    limpieza_y_o_cambio_de_filtros text NULL,
    verifi_chapas_llaves_puerta text NULL,
    observ text NULL,
    numero_de_banco text NULL,
    marca2 text NULL,
    modelo1 text NULL,
    capacidad_ah text NULL,
    estado_bat_elem_anexos_puentes text NULL,
    observ_que_bat_celdas_no text NULL,
    limpieza_general text NULL,
    observacion1 text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_radio_6_12_18_bas_cf_bb_fechacarga_idx
ON raw.radio_6_12_18_bas_cf_bb USING btree (fechacarga);
--------------------------------6
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
--------------------------------7
CREATE TABLE raw.se_banco_de_condensadores (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    existe_banco_de_condensadores text NULL,
    operan_banco_conden text NULL,
    capacidad text NULL,
    cantidad_de_pasos text NULL,
    operatividad_de_pasos text NULL,
    limpieza_con_aspiradora text NULL,
    rev_fusibl_bobina_termos text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_se_banco_de_condensadores_fechacarga_idx
ON raw.se_banco_de_condensadores USING btree (fechacarga);
--------------------------------8
CREATE TABLE raw.se_proteccion_y_pararrayos (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    local_cuenta_con_pararrayos text NULL,
    hay_algun_pararr_averia_descar text NULL,
    pararr_conect_transf text NULL,
    pararrayos_conectado_a_spat text NULL,
    compro_amarre_pararr text NULL,
    limp_oxidac_mastil_pararr text NULL,
    medir_contin_entre_transf text NULL,
    medici_pozo_tierra_conect text NULL,
    manten_seccio_e_interr_intern text NULL,
    manten_tvss_fusibl text NULL,
    ajuste_protec_rele_fusibl text NULL,
    manten_borner_platin_tierra text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_se_proteccion_y_pararrayos_fechacarga_idx
ON raw.se_proteccion_y_pararrayos USING btree (fechacarga);
--------------------------------9
CREATE TABLE raw.se_tablero_de_paso_de_salida (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    actual_planos_diagra_unifil text NULL,
    manten_preven_tabler_princi text NULL,
    limp_calibr_cambio_analiz text NULL,
    limp_calibr_cambio_transf text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_se_tablero_de_paso_de_salida_fechacarga_idx
ON raw.se_tablero_de_paso_de_salida USING btree (fechacarga);
--------------------------------10
CREATE TABLE raw.se_trafomix (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    trafom_conect_aguas_abajo text NULL,
    trafom_perten_telefo_conces text NULL,
    si_solici_corte_conces_nuestr text NULL,
    marca text NULL,
    modelo text NULL,
    capacidad_kva text NULL,
    anio text NULL,
    tipo_de_conversion_amp text NULL,
    relaci_transf_voltaj text NULL,
    sistema text NULL,
    analis_rigide_dielec text NULL,
    limp_ajuste_verif_operat text NULL,
    megado_de_trafomix_mohms text NULL,
    detecccion_fugas_a_tierra text NULL,
    trafom_conect_un_pozo text NULL,
    medici_resist_pozo_media text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_se_trafomix_fechacarga_idx
ON raw.se_trafomix USING btree (fechacarga);
--------------------------------11
CREATE TABLE raw.se_transformador_de_potencia (
    task_id varchar(255) NOT NULL,
    site_id varchar(255) NOT NULL,
    sub_wo_id varchar(255) NOT NULL,
    sistema_al_que_alimenta text NULL,
    existe_lmt_aerea text NULL,
    nivel_de_tension text NULL,
    marca text NULL,
    codigo_de_equipo text NULL,
    modelo text NULL,
    capacidad_kva text NULL,
    anio text NULL,
    estado text NULL,
    tipo_de_ventilacion text NULL,
    tipo_de_conexion text NULL,
    voltaj_alta_transf_kv text NULL,
    voltaj_baja_transf_kv text NULL,
    relaci_transf_voltaj text NULL,
    medici_resist_aislam_alta_baja text NULL,
    medici_resist_aislam_alta text NULL,
    medici_resist_aislam_baja text NULL,
    analis_rigide_dielec text NULL,
    limp_ajuste_verif_operat text NULL,
    transf_conect_un_pozo text NULL,
    marca_contin_entre_pozo_subest text NULL,
    medici_resist_pozo_media text NULL,
    verifi_que_no_este_resumi text NULL,
    observacion text NULL,
    fechacarga timestamp(0) NOT NULL
);

CREATE INDEX raw_se_transformador_de_potencia_fechacarga_idx
ON raw.se_transformador_de_potencia USING btree (fechacarga);

---12
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
CREATE TABLE raw.avr (
  task_id                                        varchar(255) NOT NULL,
  site_id                                        varchar(255) NOT NULL,
  sub_wo_id                                      varchar(255) NOT NULL,
  codigo_equipo_avr                              text NULL,
  marca                                          text NULL,
  modelo                                         text NULL,
  serie                                          text NULL,
  capacidad_kva                                  text NULL,
  voltaje_vac                                    text NULL,
  carga_kva                                      text NULL,
  anio_instalacion                               text NULL,
  revision_general_verificacion_buen_func_equipo text NULL,
  limpieza_interior_exterior_equipo              text NULL,
  ajuste_bornes_conexion_electrica               text NULL,
  avr_etiquetado_codigo_unico                    text NULL,
  observacion                                    text NULL,
  fechacarga                                     timestamp(0) NOT NULL
);

CREATE INDEX raw_avr_fechacarga_idx
ON raw.avr USING btree (fechacarga);
-----1
CREATE TABLE raw.clima_condensador (
  task_id                                         varchar(255) NOT NULL,
  site_id                                         varchar(255) NOT NULL,
  sub_wo_id                                       varchar(255) NOT NULL,

  marca_compresor_1                               text NULL,
  marca_compresor_2                               text NULL,
  codigo_unico_aire_intervenir                    text NULL,
  identificacion_nombre_aa                        text NULL,
  cantidad_condensadores_total_aa                 text NULL,
  nro_unidad_condensadora_intervenir_1            text NULL,
  tipo_aa                                         text NULL,
  marca_aa                                        text NULL,
  modelo_aa                                       text NULL,
  serie_aa                                        text NULL,
  estado_aa                                       text NULL,
  voltaje_aa                                      text NULL,
  nro_unidad_condensadora_intervenir_2            text NULL,
  refrigerante                                    text NULL,
  capacidad_tr                                    text NULL,
  ubicacion_uc                                    text NULL,
  consumo_uc_fase_r                               text NULL,
  consumo_uc_fase_s                               text NULL,
  consumo_uc_fase_t                               text NULL,
  observacion                                     text NULL,
  compresores_en_unidad_condensadora              text NULL,
  numero_compresores                              text NULL,
  rev_tension_correas_rodamientos_switch_seguridad text NULL,
  cantidad_motor_ventilador_uc                    text NULL,
  tiene_motoventilador_averiado                   text NULL,
  alimentacion_motor_ventilador                   text NULL,
  consumo_motor_ventilador_ir                     text NULL,
  consumo_motor_ventilador_is                     text NULL,
  consumo_motor_ventilador_it                     text NULL,
  temp_salida_unidad_condensadora_c               text NULL,
  tiene_visor_humedad                             text NULL,
  visor_refleja_gotas_humedad                     text NULL,
  tiene_presostato_baja                           text NULL,
  tiene_presostato_alta                           text NULL,
  rev_perdidas_aceite                             text NULL,
  revision_estado_valvulas_solen_filtro_presostato text NULL,
  observacion_condensador                         text NULL,
  recarga_gas_baja_presion_o_fuga                 text NULL,
  cantidad_refrigerante_recargado_kg              text NULL,
  peso_balon_antes_recarga_kg                     text NULL,
  peso_balon_despues_recarga_kg                   text NULL,
  cantidad_compresores_totales_aa                 text NULL,

  capacidad_compresor_1_tr                        text NULL,
  ubicacion_compresor_1                           text NULL,
  tipo_compresor_1                                text NULL,
  tipo_alimentacion_compresor_1                   text NULL,
  voltaje_compresor_1                             text NULL,
  estado_compresor_1                              text NULL,
  presion_baja_circuito_1                         text NULL,
  presion_alta_circuito_1                         text NULL,
  consumo_compresor_1_ir                          text NULL,
  consumo_compresor_1_is                          text NULL,
  consumo_compresor_1_it                          text NULL,
  observacion_compresor_1                         text NULL,

  capacidad_compresor_2_tr                        text NULL,
  ubicacion_compresor_2                           text NULL,
  tipo_compresor_2                                text NULL,
  tipo_alimentacion_compresor_2                   text NULL,
  voltaje_compresor_2                             text NULL,
  estado_compresor_2                              text NULL,
  presion_baja_circuito_2                         text NULL,
  presion_alta_circuito_2                         text NULL,
  consumo_compresor_2_ir                          text NULL,
  consumo_compresor_2_is                          text NULL,
  consumo_compresor_2_it                          text NULL,
  observacion_compresor_2                         text NULL,

  fechacarga                                      timestamp(0) NOT NULL
);

CREATE INDEX raw_clima_condensador_fechacarga_idx
ON raw.clima_condensador USING btree (fechacarga);
-----2
CREATE TABLE raw.clima_evaporador (
  task_id                                         varchar(255) NOT NULL,
  site_id                                         varchar(255) NOT NULL,
  sub_wo_id                                       varchar(255) NOT NULL,

  codigo_unico_aire_intervenir                    text NULL,
  identificacion_nombre_aa                        text NULL,
  cantidad_total_aa_sala                          text NULL,
  tipo_aa                                         text NULL,
  tipo_flujo                                      text NULL,
  marca_aa                                        text NULL,
  modelo_aa                                       text NULL,
  serie_aa                                        text NULL,
  estado_aa                                       text NULL,
  voltaje_aa                                      text NULL,
  refrigerante                                    text NULL,
  capacidad_tr                                    text NULL,
  ubicacion_ue                                    text NULL,
  comparte_ducto_con_otros_aa                     text NULL,
  consumo_ue_fase_r                               text NULL,
  consumo_ue_fase_s                               text NULL,
  consumo_ue_fase_t                               text NULL,
  observacion_ue                                  text NULL,
  compresores_en_unidad_evaporadora               text NULL,
  numero_compresores                              text NULL,
  porcentaje_carga                                text NULL,
  temp_inyeccion_c                                text NULL,
  temp_retorno_c                                  text NULL,
  temp_seteo_c                                    text NULL,
  revision_motor_fan_y_giro                       text NULL,
  limpieza_turbinas_ventilador_evap               text NULL,
  medidas_filtro_y_cantidad_cm                    text NULL,
  filtro_aire_limpio_o_cambiado                   text NULL,
  lavado_serpentin_alki_klean                     text NULL,
  consumo_motor_ventilador_ir                     text NULL,
  consumo_motor_ventilador_is                     text NULL,
  consumo_motor_ventilador_it                     text NULL,
  limpieza_ajuste_terminales_ue_con_motoventilador text NULL,
  rev_tension_correas_rodamientos_switch_seguridad text NULL,
  revision_ajuste_lubricacion_alineamiento_poleas text NULL,
  observacion_filtros_rodamientos_poleas_correas_giro_ventilador text NULL,
  rev_fusibles_conexiones_electricas              text NULL,
  rev_calibracion_termostatos_humidificadores     text NULL,
  sala_gestionada_alarma_alta_temperatura         text NULL,
  temp_alarma_seteada_c                           text NULL,
  temp_ambiente_c                                 text NULL,
  prueba_equipo_principal_y_respaldo              text NULL,
  limpieza_ajuste_terminales_ue                   text NULL,
  observacion_sist_electrico                      text NULL,
  limpieza_bandeja_condensado_y_drenaje           text NULL,
  agua_fluye_normal_en_bandeja                    text NULL,
  mantenimiento_bomba_condensado                  text NULL,
  observacion_drenaje                             text NULL,
  cantidad_compresores_totales_aa                 text NULL,

  -- Compresor 1
  capacidad_compresor_1_tr                        text NULL,
  ubicacion_compresor_1                           text NULL,
  tipo_compresor_1                                text NULL,
  tipo_alimentacion_compresor_1                   text NULL,
  voltaje_compresor_1                             text NULL,
  estado_compresor_1                              text NULL,
  presion_baja_circuito_1                         text NULL,
  presion_alta_circuito_1                         text NULL,
  consumo_compresor_1_ir                          text NULL,
  consumo_compresor_1_is                          text NULL,
  consumo_compresor_1_it                          text NULL,
  observacion_compresor_1                         text NULL,

  -- Compresor 2
  capacidad_compresor_2_tr                        text NULL,
  ubicacion_compresor_2                           text NULL,
  tipo_compresor_2                                text NULL,
  marca_compresor_1                               text NULL, -- viene así en el formulario
  tipo_alimentacion_compresor_2                   text NULL,
  marca_compresor_2                               text NULL,
  voltaje_compresor_2                             text NULL,
  estado_compresor_2                              text NULL,
  presion_baja_circuito_2                         text NULL,
  presion_alta_circuito_2                         text NULL,
  consumo_compresor_2_ir                          text NULL,
  consumo_compresor_2_is                          text NULL,
  consumo_compresor_2_it                          text NULL,
  observacion_compresor_2                         text NULL,

  fechacarga                                      timestamp(0) NOT NULL
);

CREATE INDEX raw_clima_evaporador_fechacarga_idx
ON raw.clima_evaporador USING btree (fechacarga);
-------3
CREATE TABLE raw.clima (
  task_id                                   varchar(255) NOT NULL,
  site_id                                   varchar(255) NOT NULL,
  sub_wo_id                                 varchar(255) NOT NULL,

  -- Bloque 1: datos generales equipo / condensador
  ident_nombre_aa_1                         text NULL,
  marca_aa_1                                text NULL,
  modelo_aa_1                               text NULL,
  serie_aa_1                                text NULL,
  refrigerante_1                            text NULL,
  capacidad_tr_1                            text NULL,
  codigo_numero_equipo_1                    text NULL,
  ubicacion_1                               text NULL,
  numero_condensador_1                      text NULL,
  observacion_1                             text NULL,

  -- Bloque 2: condensador / motores / compresores (parte 1)
  ident_nombre_aa_2                         text NULL,
  numero_condensador_2                      text NULL,
  limpieza_turbinas_motores                 text NULL,
  rev_correas_rodamientos_switch_1          text NULL,
  consumo_motor_ventilador_ir_is_1          text NULL,
  verificacion_temp_inyeccion_retorno_1     text NULL,
  rev_ajuste_alineamiento_transmision_1     text NULL,
  rev_nivel_aceite_refrigerante_1           text NULL,
  rev_switch_seguridad_1                    text NULL,
  consumo_electrico_compresor1_1            text NULL,
  consumo_electrico_compresor2_1            text NULL,
  observacion_2                             text NULL,
  limpieza_motor_ventiladores_1             text NULL,
  limpieza_serpentin_1                      text NULL,
  rev_rodamientos_1                         text NULL,
  consumo_electrico_condensador_motor1      text NULL,
  consumo_electrico_condensador_motor2      text NULL,
  rev_alineamiento_tensado_fajas_1          text NULL,
  presion_baja_circuito1_1                  text NULL,
  presion_baja_circuito2_1                  text NULL,
  presion_alta_circuito1_1                  text NULL,
  presion_alta_circuito2_1                  text NULL,
  rev_perdidas_aceite_1                     text NULL,
  lavado_evaporadores_1                     text NULL,
  rev_estado_valvulas_filtro_presostato_1   text NULL,
  recarga_gas_baja_presion_o_fuga_1         text NULL,
  observacion_3                             text NULL,

  -- Bloque 3: pruebas de compresor
  ident_nombre_aa_3                         text NULL,
  numero_compresor                          text NULL,
  ubicacion_compresor                       text NULL,
  tipo_compresor                            text NULL,
  consumo_compresor_irs_ist_itr             text NULL,
  consumo_compresor_arranque_irs_ist_itr    text NULL,
  resistencia_bobinas_ohm                   text NULL,
  medicion_continuidad_bobinas              text NULL,
  observacion_4                             text NULL,

  -- Bloque 4: filtros / humidificador / bandeja / ventiladores
  ident_nombre_aa_4                         text NULL,
  numero_equipo_2                           text NULL,
  tipo_equipo_2                             text NULL,
  marca_equipo_2                            text NULL,
  modelo_equipo_2                           text NULL,
  serie_equipo_2                            text NULL,
  capacidad_tr_2                            text NULL,
  ubicacion_equipo_2                        text NULL,
  observacion_5                             text NULL,
  limpieza_cambio_filtro                    text NULL,
  rev_switch_filtros_tapados                text NULL,
  medidas_filtro_y_cantidad                 text NULL,
  lavado_evaporadores_2                     text NULL,
  limpieza_deshumidificador                 text NULL,
  limpieza_bandeja_bomba_condensado         text NULL,
  limpieza_bandeja                          text NULL,
  calidad_agua                              text NULL,
  rev_valvula_flotador_drenaje_lampara      text NULL,
  consumo_electrico_humidificador           text NULL,
  limpieza_pruebas_proteccion_humidificador text NULL,
  limpieza_turbinas_2                       text NULL,
  rev_correas_rodamientos_switch_2          text NULL,
  consumo_motor_ventilador_ir_is_2          text NULL,
  verificacion_temp_inyeccion_retorno_2     text NULL,
  rev_ajuste_alineamiento_poleas_anclaje    text NULL,
  rev_ajuste_alineamiento_transmision_2     text NULL,
  observacion_6                             text NULL,
  rev_reparacion_serpentin_obstruccion_1    text NULL,
  rev_perdidas_aceite_2                     text NULL,
  observacion_7                             text NULL,
  rev_reparacion_serpentin_obstruccion_2    text NULL,
  limpieza_bandeja_condensado_2             text NULL,
  presion_succion_circuito1                 text NULL,
  presion_succion_circuito2                 text NULL,
  presion_descarga_circuito1                text NULL,
  presion_descarga_circuito2                text NULL,
  rev_perdidas_aceite_3                     text NULL,
  lavado_evaporadores_3                     text NULL,
  observacion_8                             text NULL,

  -- Bloque 5: sistema eléctrico / control / alarmas
  ident_nombre_aa_5                         text NULL,
  rev_fusibles_conexiones_electricas        text NULL,
  rev_calibracion_termostatos_humidificadores text NULL,
  verificacion_protecciones_reles_contactores text NULL,
  rev_sistema_operacion_secuencial_control  text NULL,
  alarma_termostato_alta_temp               text NULL,
  sala_con_alarma_alta_temp                 text NULL,
  temp_seteada_c                            text NULL,
  temp_ambiente_c                           text NULL,
  prueba_equipo_principal_respaldo          text NULL,
  observacion_9                             text NULL,

  fechacarga                                timestamp(0) NOT NULL
);

CREATE INDEX raw_clima_fechacarga_idx
ON raw.clima USING btree (fechacarga);
-------4
CREATE TABLE raw.inversor (
  task_id                                   varchar(255) NOT NULL,
  site_id                                   varchar(255) NOT NULL,
  sub_wo_id                                 varchar(255) NOT NULL,

  codigo_unico_equipo_inversor              text NULL,
  marca                                     text NULL,
  modelo                                    text NULL,
  serie                                     text NULL,
  capacidad_kva                             text NULL,
  voltaje_vac                               text NULL,
  carga_kva                                 text NULL,
  anio_instalacion                          text NULL,
  revision_general_verificacion_buen_func_equipo text NULL,
  ajuste_bornes_conexion_electrica          text NULL,
  limpieza_revision_calibracion_equipo      text NULL,
  inversor_etiquetado_codigo_unico          text NULL,
  observacion                               text NULL,

  fechacarga                                timestamp(0) NOT NULL
);

CREATE INDEX raw_inversor_fechacarga_idx
ON raw.inversor USING btree (fechacarga);
-----5
CREATE TABLE raw.lmt_lbt_conductores_y_protecc (
  task_id                                              varchar(255) NOT NULL,
  site_id                                              varchar(255) NOT NULL,
  sub_wo_id                                            varchar(255) NOT NULL,

  conductor                                            text NULL,
  tipo_conductor                                       text NULL,
  tipo_instalacion                                     text NULL,
  limpieza_ajuste_equip_mecanico_electrico_bt_mt       text NULL,
  medicion_aislamiento_linea                           text NULL,
  medicion_aislamiento_pararrayos                      text NULL,
  existen_pararrayos                                   text NULL,
  estado_pararrayos                                    text NULL,
  existen_spat_pararrayos                              text NULL,
  pararrayos_conectado_tierra_pararrayos               text NULL,
  medicion_resistencia_spat_ohm                        text NULL,
  dms                                                  text NULL,
  medicion_dist_min_seguridad_vertical_horizontal_m    text NULL,
  faja_servidumbre                                     text NULL,
  empalme_aereo                                        text NULL,
  empalme_subterraneo                                  text NULL,
  observacion                                          text NULL,

  fechacarga                                           timestamp(0) NOT NULL
);

CREATE INDEX raw_lmt_lbt_conductores_y_protecc_fechacarga_idx
ON raw.lmt_lbt_conductores_y_protecc USING btree (fechacarga);
-----6
CREATE TABLE raw.lmt_lbt_datos_de_linea_genera (
  task_id                             varchar(255) NOT NULL,
  site_id                             varchar(255) NOT NULL,
  sub_wo_id                           varchar(255) NOT NULL,

  tipo_linea                          text NULL,
  ubicacion_geografica                text NULL,
  tipo_conexion                       text NULL,
  nivel_tension                       text NULL,
  cantidad_postes_toda_linea          text NULL,
  tipo_poste                          text NULL,
  longitud_toda_linea_m               text NULL,
  ubicacion_punto_entrega             text NULL,

  fechacarga                          timestamp(0) NOT NULL
);

CREATE INDEX raw_lmt_lbt_datos_de_linea_genera_fechacarga_idx
ON raw.lmt_lbt_datos_de_linea_genera USING btree (fechacarga);
-----7
CREATE TABLE raw.lmt_lbt_poste_de_cada_uno (
  task_id      varchar(255) NOT NULL,
  site_id      varchar(255) NOT NULL,
  sub_wo_id    varchar(255) NOT NULL,
  fechacarga   timestamp(0) NOT NULL
);

CREATE INDEX raw_lmt_lbt_poste_de_cada_uno_fechacarga_idx
ON raw.lmt_lbt_poste_de_cada_uno USING btree (fechacarga);
-----8
CREATE TABLE raw.mantenimiento_preventivo_dinami (
  task_id      varchar(255) NOT NULL,
  site_id      varchar(255) NOT NULL,
  sub_wo_id    varchar(255) NOT NULL,
  work_order   text NULL,
  fechacarga   timestamp(0) NOT NULL
);

CREATE INDEX raw_mantenimiento_preventivo_dinami_fechacarga_idx
ON raw.mantenimiento_preventivo_dinami USING btree (fechacarga);
-----9

CREATE TABLE raw.mantenimiento_preventivo (
  task_id                                   varchar(255) NOT NULL,
  site_id                                   varchar(255) NOT NULL,
  sub_wo_id                                 varchar(255) NOT NULL,

  mantenimiento_preventivo_nombre_local     text NULL,

  -- ENERGIA - Rectificadores
  energia_rectif_cantidad_modulos           text NULL,
  energia_rectif_carga_actual_dc            text NULL,
  energia_rectif_observaciones              text NULL,
  energia_rectif_detalle_observaciones      text NULL,

  -- ENERGIA - Baterias
  energia_bat_cuenta_con_banco              text NULL,
  energia_bat_tipo                          text NULL,
  energia_bat_cantidad_bancos               text NULL,
  energia_bat_cantidad_celdas               text NULL,
  energia_bat_marca                         text NULL,
  energia_bat_observaciones                 text NULL,
  energia_bat_detalle_observaciones         text NULL,

  -- INFRA - Torre
  infra_estado_torre                        text NULL,
  infra_torre_tiene_observaciones           text NULL,
  infra_torre_detalle_observacion           text NULL,

  -- INFRA - Sellado caseta/sala
  infra_sellado_caseta_sala                 text NULL,
  infra_sellado_tiene_observaciones         text NULL,
  infra_sellado_detalle_observacion         text NULL,

  -- INFRA - Hermetizado puerta
  infra_hermetizado_puerta                  text NULL,
  infra_hermetizado_tiene_observaciones     text NULL,
  infra_hermetizado_detalle_observacion     text NULL,

  -- INFRA - Filtraciones caseta/sala
  infra_filtraciones_caseta_sala            text NULL,
  infra_filtraciones_tiene_observaciones    text NULL,
  infra_filtraciones_detalle_observacion    text NULL,

  -- INFRA - Cableado alarmas
  infra_cableado_alarmas                    text NULL,
  infra_cableado_tiene_observaciones        text NULL,
  infra_cableado_detalle_observacion        text NULL,

  -- INFRA - Sensores caseta/sala
  infra_sensores_caseta_sala                text NULL,
  infra_sensores_tiene_observaciones        text NULL,
  infra_sensores_detalle_observacion        text NULL,

  -- INFRA - Coubicación otros operadores
  infra_coubicacion_otros_operadores        text NULL,
  infra_coubicacion_tiene_observaciones     text NULL,
  infra_coubicacion_detalle_observacion     text NULL,

  -- TABLEROS DE ENERGIA
  tableros_energia_estado                   text NULL,
  tableros_energia_tiene_observaciones      text NULL,
  tableros_energia_detalle_observacion      text NULL,

  -- GRUPO ELECTROGENO
  grupo_electrogeno_hay                     text NULL,
  grupo_electrogeno_tiene_observaciones     text NULL,
  grupo_electrogeno_detalle_observacion     text NULL,

  -- SISTEMA PUESTA A TIERRA
  sistema_pt_hay_placas_igb_egb_mgb         text NULL,
  sistema_pt_tiene_observaciones            text NULL,
  sistema_pt_detalle_observacion            text NULL,

  -- GULNR
  gulnr_bbu_rtn                             text NULL,
  gulnr_estado_peinado_fo_red_energia       text NULL,
  gulnr_tiene_observaciones                 text NULL,
  gulnr_detalle_observacion                 text NULL,

  -- OBSERVACION GENERAL
  observacion_general                       text NULL,

  -- ENERGIA - Unidades de Distribuciones
  energia_unid_distrib_en_buen_estado       text NULL,
  energia_unid_distrib_tiene_observaciones  text NULL,
  energia_unid_distrib_detalle_observaciones text NULL,

  -- CLIMA - Aire Acondicionado
  clima_aa_cuenta_con_aa                    text NULL,
  clima_aa_tipo                             text NULL,
  clima_aa_cantidad                         text NULL,
  clima_aa_marca                            text NULL,
  clima_aa_observaciones                    text NULL,
  clima_aa_detalle_observaciones            text NULL,

  fechacarga                                timestamp(0) NOT NULL
);

CREATE INDEX raw_mantenimiento_preventivo_fechacarga_idx
ON raw.mantenimiento_preventivo USING btree (fechacarga);
----10
CREATE TABLE raw.ups_bateria_de_ups (
  task_id                                      varchar(255) NOT NULL,
  site_id                                      varchar(255) NOT NULL,
  sub_wo_id                                    varchar(255) NOT NULL,

  codigo_unico_banco_bateria                   text NULL,
  codigo_ups_asociado_banco_bat                text NULL,
  nombre_ups_asociado_banco_bat                text NULL,
  marca_ups_asociado_banco_bat                 text NULL,
  capacidad_ups_asociado_banco_bat_ampdc       text NULL,
  tipo_banco_baterias                          text NULL,
  subtipo_banco_baterias                       text NULL,
  nombre_sala_bateria                          text NULL,
  numero_banco                                 text NULL,
  cantidad_celdas                              text NULL,
  voltaje_total_banco_v                        text NULL,
  voltaje_unitario_celda_v                     text NULL,
  marca                                        text NULL,
  modelo                                       text NULL,
  estado_banco_baterias                        text NULL,
  tiene_bornes_levantados                      text NULL,
  tiene_celda_rajada                           text NULL,
  capacidad_ah                                 text NULL,
  fecha_instalacion                            text NULL,
  cantidad_cables_por_polo                     text NULL,
  calibre_cable                                text NULL,
  temperatura_c                                text NULL,
  medir_voltaje_cada_celda_num_celda_averiada  text NULL,
  engrase                                      text NULL,
  ajustes_reapriete_general                    text NULL,
  limpieza_general_bornes_elementos            text NULL,
  voltaje_flotacion                            text NULL,
  corriente_recarga                            text NULL,
  estado_baterias_elementos_anexos             text NULL,
  estado_bornes_cada_bateria_celda             text NULL,
  estado_puentes_entre_baterias_celdas         text NULL,
  observar_deformacion_baterias_celdas         text NULL,
  medicion_impedancia                          text NULL,
  autonomia_prueba_descarga_controlada         text NULL,
  observacion                                  text NULL,
  banco_bateria_etiquetado_codigo_unico        text NULL,

  fechacarga                                   timestamp(0) NOT NULL
);

CREATE INDEX raw_ups_bateria_de_ups_fechacarga_idx
ON raw.ups_bateria_de_ups USING btree (fechacarga);
------11
CREATE TABLE raw.ups_ups (
  task_id                                      varchar(255) NOT NULL,
  site_id                                      varchar(255) NOT NULL,
  sub_wo_id                                    varchar(255) NOT NULL,

  codigo_unico_ups                             text NULL,
  tipo                                         text NULL,
  configuracion_ups                            text NULL,
  marca                                        text NULL,
  modelo                                       text NULL,
  serie                                        text NULL,
  fases                                        text NULL,
  capacidad_kva                                text NULL,
  carga_display_amp_ac                         text NULL,
  carga_pinza_amp_ac                           text NULL,
  anio_instalacion                             text NULL,
  estado_ups                                   text NULL,
  modo_trabajo                                 text NULL,
  revision_general_verificacion_buen_func_equipo text NULL,
  limpieza_general_terminales_electricos       text NULL,
  ajuste_bornes_conexion_electrica_partes_mecanicas text NULL,
  limpieza_revision_calibracion_equipo         text NULL,
  limpieza_revision_calibracion_modulos_inversor_rectif_switch text NULL,
  inspeccion_general_banco_baterias            text NULL,
  limpieza_engrase_terminales                  text NULL,
  verificacion_frecuencia_entrada_salida       text NULL,
  voltaje_entrada_salida                       text NULL,
  corriente_entrada_salida                     text NULL,
  medicion_armonicos_corriente_voltaje         text NULL,
  prueba_descarga_carga_controlada_banco       text NULL,
  medicion_temp_ambiente                       text NULL,
  medicion_voltaje_flotante_banco_por_celda    text NULL,
  medicion_nivel_rizado_rectificador           text NULL,
  ups_etiquetado_codigo_unico                  text NULL,
  diagrama_unifilar                            text NULL,
  observacion                                  text NULL,

  fechacarga                                   timestamp(0) NOT NULL
);

CREATE INDEX raw_ups_ups_fechacarga_idx
ON raw.ups_ups USING btree (fechacarga);
----12
---funcion 
-- DROP PROCEDURE ods.sp_validacion_hm_checklist(date);

-- DROP PROCEDURE ods.sp_validacion_hm_checklist(date);

CREATE OR REPLACE PROCEDURE ods.sp_validacion_hm_checklist(IN p_fecha date DEFAULT CURRENT_DATE)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio    timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp     integer      := 'ods.sp_validacion_hm_checklist(date)'::regprocedure::oid::int;
  v_sp_name   text         := 'ods.sp_validacion_hm_checklist(date)'::regprocedure::text;

  v_inserted  integer := 0;
  v_updated   integer := 0;
  v_deleted   integer := 0;
  v_nulls     integer := NULL;
  v_estado    varchar(50);
  v_msj       text;

  v_tablas CONSTANT text[] := ARRAY[
    -- CF / IE (ya existentes)
    'raw.cf_banco_de_baterias',
    'raw.cf_bastidor_distribucion',
    'raw.cf_cuadro_de_fuerza',
    'raw.cf_modulos_rectificadores',
    'raw.cf_tablero_ac_de_cuadro_de_fu',
    'raw.cf_descarga_controlada_bater',
    'raw.ie_datos_spat_general',
    'raw.ie_mantenimiento_pozo_por_poz',
    'raw.ie_suministro_de_energia',
    'raw.ie_tablero_principal',
    'raw.ie_tablero_secundario',

    -- SOLARES
    'raw.sol_banco_de_baterias_solares',
    'raw.sol_controlador_solar',
    'raw.sol_informacion_general_sist',
    'raw.sol_paneles_solares',

    -- TRANSMISIÓN / TX
    'raw.tx_bh_2_4_6_12_gwc',
    'raw.tx_bh_2_4_6_12_gwd',
    'raw.tx_bh_2_4_6_12_gwt',
    'raw.tx_2_4_6_12_antena',
    'raw.tx_2_4_6_12_dwdm',
    'raw.tx_2_4_6_12_idu',
    'raw.tx_2_4_6_12_odu',
    'raw.tx_2_4_6_12_sdh',

    -- GE
    'raw.ge_grupo_electrogeno',
    'raw.ge_info_general_de_tanque',
    'raw.ge_limp_interna_tk',
    'raw.ge_tablero_de_transferencia_a',
    'raw.ge_transferencia_con_carga',

    -- RADIO
    'raw.radio_6_12_18_bas_cf_bb',
    'raw.radio_6_12_18_bbu',

    -- SE
    'raw.se_banco_de_condensadores',
    'raw.se_proteccion_y_pararrayos',
    'raw.se_tablero_de_paso_de_salida',
    'raw.se_trafomix',
    'raw.se_transformador_de_potencia',

    -- UPS / INVERSOR / AVR
    'raw.ups_ups',
    'raw.ups_bateria_de_ups',
    'raw.inversor',
    'raw.avr',

    -- CLIMA
    'raw.clima',
    'raw.clima_condensador',
    'raw.clima_evaporador',

    -- LÍNEA MT/BT
    'raw.lmt_lbt_conductores_y_protecc',
    'raw.lmt_lbt_datos_de_linea_genera',
    'raw.lmt_lbt_poste_de_cada_uno',

    -- MANTENIMIENTO
    'raw.mantenimiento_preventivo',
    'raw.mantenimiento_preventivo_dinami'
  ];

  v_t        text;
  v_keep_ts  timestamp;
  v_borradas bigint;
  v_detalle  text := '';
BEGIN
  FOREACH v_t IN ARRAY v_tablas LOOP
    BEGIN
      -- 1) Asegurar índice por fechacarga
      EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %s (fechacarga)',
        replace(v_t, '.', '_') || '_fechacarga_idx', v_t
      );

      -- 2) Obtener el máximo fechacarga del día p_fecha
      EXECUTE format(
        'SELECT MAX(fechacarga) FROM %s WHERE fechacarga::date = $1', v_t
      ) USING p_fecha INTO v_keep_ts;

      IF v_keep_ts IS NULL THEN
        v_detalle := v_detalle || format('%s: sin filas %s; ', v_t, p_fecha);
        CONTINUE;
      END IF;

      -- 3) Borrar duplicados del mismo día, conservando solo el último lote
      EXECUTE format(
        'DELETE FROM %s WHERE fechacarga::date = $1 AND fechacarga <> $2', v_t
      ) USING p_fecha, v_keep_ts;

      GET DIAGNOSTICS v_borradas = ROW_COUNT;
      v_deleted := v_deleted + COALESCE(v_borradas,0);

      v_detalle := v_detalle
        || format('%s: keep=%s; borradas=%s; ',
                  v_t, to_char(v_keep_ts,'YYYY-MM-DD HH24:MI:SS'), v_borradas);
    EXCEPTION
      WHEN undefined_column THEN
        v_detalle := v_detalle || format('%s: ERROR sin columna fechacarga; ', v_t);
      WHEN OTHERS THEN
        v_detalle := v_detalle || format('%s: ERROR %s; ', v_t, SQLERRM);
    END;
  END LOOP;

  v_estado := 'DONE';
  v_msj := format('Validación HM Checklist (fecha=%s). Borradas=%s. %s',
                  p_fecha, v_deleted, v_detalle);

  CALL public.sp_grabar_log_sp(
    p_id_sp      => v_id_sp,
    p_inicio     => v_inicio,
    p_fin        => clock_timestamp()::timestamp(0),
    p_inserted   => v_inserted,
    p_updated    => v_updated,
    p_deleted    => v_deleted,
    p_nulls      => v_nulls,
    p_estado     => v_estado,
    p_msj_error  => v_msj,
    p_sp         => v_sp_name
  );
EXCEPTION
  WHEN OTHERS THEN
    CALL public.sp_grabar_log_sp(
      p_id_sp      => v_id_sp,
      p_inicio     => v_inicio,
      p_fin        => clock_timestamp()::timestamp(0),
      p_inserted   => v_inserted,
      p_updated    => v_updated,
      p_deleted    => v_deleted,
      p_nulls      => v_nulls,
      p_estado     => 'ERROR',
      p_msj_error  => SQLERRM,
      p_sp         => v_sp_name
    );
    RAISE;
END;
$procedure$;
-----


CREATE OR REPLACE PROCEDURE ods.sp_cargar_web_md_neteco()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio   timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp    integer      := 'ods.sp_cargar_web_md_neteco()'::regprocedure::oid::int;
  v_sp_name  text         := 'ods.sp_cargar_web_md_neteco()'::regprocedure::text;

  v_ins_dia  integer := 0;
  v_upd_dia  integer := 0;
  v_ins_mes  integer := 0;
  v_upd_mes  integer := 0;

  v_estado   varchar(50);
  v_msj      text;
BEGIN
  /* ========= 1) LIMPIEZA + DEDUP POR HORA (TEMP) ========= */
  CREATE TEMP TABLE tmp_neteco_hora_dedup
  ON COMMIT DROP AS
  WITH base AS (
    SELECT
      NULLIF(NULLIF(btrim(r.site_name), 'NaN'), '')::text AS site_name,
      NULLIF(NULLIF(btrim(r.subnet), 'NaN'), '')::text AS subnet,
      NULLIF(NULLIF(btrim(r.manage_object), 'NaN'), '')::text AS manage_object,

      CASE
        WHEN r.start_time IS NULL
          OR btrim(r.start_time) = ''
          OR lower(btrim(r.start_time)) IN ('nan','null','-','n/a')
        THEN NULL
        ELSE r.start_time::timestamp
      END AS start_time,

      public.try_numeric(
        CASE
          WHEN r.energy_consumption_per_hour_kwh IS NULL
            OR btrim(r.energy_consumption_per_hour_kwh) = ''
            OR lower(btrim(r.energy_consumption_per_hour_kwh)) IN ('nan','null','-','n/a')
          THEN NULL
          ELSE btrim(r.energy_consumption_per_hour_kwh)
        END
      )::numeric(18,4) AS energy_consumption_per_hour,

      public.try_numeric(
        CASE
          WHEN r.supply_duration_per_hour_h IS NULL
            OR btrim(r.supply_duration_per_hour_h) = ''
            OR lower(btrim(r.supply_duration_per_hour_h)) IN ('nan','null','-','n/a')
          THEN NULL
          ELSE btrim(r.supply_duration_per_hour_h)
        END
      )::numeric(18,4) AS supply_duration_per_hour,

      public.try_numeric(
        CASE
          WHEN r.total_energy_consumption_kwh IS NULL
            OR btrim(r.total_energy_consumption_kwh) = ''
            OR lower(btrim(r.total_energy_consumption_kwh)) IN ('nan','null','-','n/a')
          THEN NULL
          ELSE btrim(r.total_energy_consumption_kwh)
        END
      )::numeric(18,4) AS total_energy_consumption
    FROM raw.web_md_neteco r
  )
  SELECT
    site_name,
    manage_object,
    start_time,
    MAX(subnet)                      AS subnet,
    MAX(energy_consumption_per_hour) AS energy_consumption_per_hour,
    MAX(supply_duration_per_hour)    AS supply_duration_per_hour,
    MAX(total_energy_consumption)    AS total_energy_consumption
  FROM base
  WHERE site_name IS NOT NULL
    AND manage_object IS NOT NULL
    AND start_time IS NOT NULL
  GROUP BY site_name, manage_object, start_time;

  /* ========= 2) UPSERT DIARIO (SIN TRUNCATE) ========= */
  WITH daily AS (
    SELECT
      site_name,
      manage_object,
      start_time::date AS fecha,
      MAX(subnet) AS subnet,
      SUM(energy_consumption_per_hour) AS energy_consumption_per_day_kwh,
      SUM(supply_duration_per_hour)    AS supply_duration_per_day_h,
      SUM(total_energy_consumption)    AS total_energy_consumption_per_day_kwh
    FROM tmp_neteco_hora_dedup
    WHERE start_time::date <= CURRENT_DATE   -- cambia a < CURRENT_DATE si quieres excluir hoy
    GROUP BY site_name, manage_object, start_time::date
  ),
  upsert_diaria AS (
    INSERT INTO ods.web_hd_neteco_diaria (
      site_name,
      manage_object,
      subnet,
      fecha,
      energy_consumption_per_day_kwh,
      supply_duration_per_day_h,
      total_energy_consumption_per_day_kwh
    )
    SELECT
      site_name,
      manage_object,
      subnet,
      fecha,
      energy_consumption_per_day_kwh,
      supply_duration_per_day_h,
      total_energy_consumption_per_day_kwh
    FROM daily
    ON CONFLICT (site_name, manage_object, fecha)
    DO UPDATE SET
      subnet = EXCLUDED.subnet,
      energy_consumption_per_day_kwh = EXCLUDED.energy_consumption_per_day_kwh,
      supply_duration_per_day_h      = EXCLUDED.supply_duration_per_day_h,
      total_energy_consumption_per_day_kwh = EXCLUDED.total_energy_consumption_per_day_kwh
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    COALESCE(COUNT(*) FILTER (WHERE inserted), 0),
    COALESCE(COUNT(*) FILTER (WHERE NOT inserted), 0)
  INTO v_ins_dia, v_upd_dia
  FROM upsert_diaria;

  /* ========= 3) UPSERT MENSUAL (SIN TRUNCATE) ========= */
  WITH monthly AS (
    SELECT
      site_name,
      manage_object,
      date_trunc('month', start_time)::date AS mes, -- cumple CHECK day=1
      MAX(subnet) AS subnet,
      SUM(energy_consumption_per_hour) AS energy_consumption_per_month_kwh,
      SUM(supply_duration_per_hour)    AS supply_duration_per_month_h,
      SUM(total_energy_consumption)    AS total_energy_consumption_per_month_kwh
    FROM tmp_neteco_hora_dedup
    WHERE start_time::date <= CURRENT_DATE   -- cambia a < CURRENT_DATE si quieres excluir hoy
    GROUP BY site_name, manage_object, date_trunc('month', start_time)::date
  ),
  upsert_mensual AS (
    INSERT INTO ods.web_hd_neteco_mensual (
      site_name,
      manage_object,
      subnet,
      mes,
      energy_consumption_per_month_kwh,
      supply_duration_per_month_h,
      total_energy_consumption_per_month_kwh
    )
    SELECT
      site_name,
      manage_object,
      subnet,
      mes,
      energy_consumption_per_month_kwh,
      supply_duration_per_month_h,
      total_energy_consumption_per_month_kwh
    FROM monthly
    ON CONFLICT (site_name, manage_object, mes)
    DO UPDATE SET
      subnet = EXCLUDED.subnet,
      energy_consumption_per_month_kwh = EXCLUDED.energy_consumption_per_month_kwh,
      supply_duration_per_month_h      = EXCLUDED.supply_duration_per_month_h,
      total_energy_consumption_per_month_kwh = EXCLUDED.total_energy_consumption_per_month_kwh
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    COALESCE(COUNT(*) FILTER (WHERE inserted), 0),
    COALESCE(COUNT(*) FILTER (WHERE NOT inserted), 0)
  INTO v_ins_mes, v_upd_mes
  FROM upsert_mensual;

  /* ========= 4) LOG ========= */
  v_estado := 'DONE';
  v_msj := format(
    'RAW -> DIARIA/MENSUAL (UPSERT). Diaria: Insert=%s Update=%s | Mensual: Insert=%s Update=%s | Origen: raw.web_md_neteco.',
    COALESCE(v_ins_dia,0), COALESCE(v_upd_dia,0),
    COALESCE(v_ins_mes,0), COALESCE(v_upd_mes,0)
  );

  CALL public.sp_grabar_log_sp(
    p_id_sp      => v_id_sp,
    p_inicio     => v_inicio,
    p_fin        => clock_timestamp()::timestamp(0),
    p_inserted   => (COALESCE(v_ins_dia,0) + COALESCE(v_ins_mes,0)),
    p_updated    => (COALESCE(v_upd_dia,0) + COALESCE(v_upd_mes,0)),
    p_deleted    => NULL::integer,
    p_nulls      => NULL::integer,
    p_estado     => v_estado,
    p_msj_error  => v_msj,
    p_sp         => v_sp_name
  );

EXCEPTION
  WHEN OTHERS THEN
    v_estado := 'ERROR';
    v_msj    := SQLERRM;

    CALL public.sp_grabar_log_sp(
      p_id_sp      => v_id_sp,
      p_inicio     => v_inicio,
      p_fin        => clock_timestamp()::timestamp(0),
      p_inserted   => NULL::integer,
      p_updated    => NULL::integer,
      p_deleted    => NULL::integer,
      p_nulls      => NULL::integer,
      p_estado     => v_estado,
      p_msj_error  => v_msj,
      p_sp         => v_sp_name
    );

    RAISE;
END;
$procedure$
;