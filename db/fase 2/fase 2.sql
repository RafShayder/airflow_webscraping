----raw 

CREATE TABLE raw.sftp_hd_toa (
	tecnico text NULL,
	id_recurso text NULL,
	nro_toa text NULL,
	subtipo_de_actividad text NULL,
	numero_de_peticion text NULL,
	fecha_de_cita text NULL,
	sla_inicio text NULL,
	sla_fin text NULL,
	localidad text NULL,
	direccion text NULL,
	direccion_polar_x text NULL,
	direccion_polar_y text NULL,
	nombre_cliente text NULL,
	hora_de_asignacion_de_actividad text NULL,
	fecha_de_registro_de_actividad_toa text NULL,
	notas text NULL,
	codigo_de_cliente text NULL,
	fecha_hora_de_cancelacion text NULL,
	empresa text NULL,
	bucket_inicial text NULL,
	usuario_iniciado text NULL,
	nombre_distrito text NULL,
	sistema_origen text NULL,
	id_del_ticket text NULL,
	quiebres text NULL,
	fecha_de_inicio_pint text NULL,
	inicio_pr1 text NULL,
	fin_pr1 text NULL,
	fin_pr2 text NULL,
	inicio_pr2 text NULL,
	fin_pr3 text NULL,
	inicio_pr3 text NULL,
	fin_pr4 text NULL,
	inicio_pr4 text NULL,
	motivo_pr1 text NULL,
	motivo_pr2 text NULL,
	motivo_pr3 text NULL,
	motivo_pr4 text NULL,
	nombre_local text NULL,
	tipo_de_local text NULL,
	zona_geografica text NULL,
	zona text NULL,
	estado_toa text NULL,
	duracion_pr1 text NULL,
	duracion_pr2 text NULL,
	duracion_pr3 text NULL,
	duracion_pr4 text NULL,
	estado text NULL,
	codigo_completado_reparacion_y_preventivo_cable text NULL,
	cliente_acepta_solucion_anticipada_de_reclamo text NULL,
	averia_efectiva_marca_sar_t text NULL
);
---------
CREATE TABLE raw.sftp_mm_pago_energia (
	nota_al_aprobador_char_40 varchar(255) NULL,
	concesionario varchar(255) NULL,
	ruc_concesionario varchar(255) NULL,
	numero_suministro varchar(255) NULL,
	recibo varchar(255) NULL,
	total_pagado varchar(255) NULL,
	fecha_emision varchar(255) NULL,
	fecha_vencimiento varchar(255) NULL,
	periodo_consumo varchar(255) NULL,
	archivo varchar(255) NULL
);
-------
CREATE TABLE IF NOT EXISTS raw.web_mm_autin_infogeneral (
    task_id TEXT,
    createtime TEXT,
    dispatch_time TEXT,
    accept_time TEXT,
    depart_time TEXT,
    arrive_time TEXT,
    complete_time TEXT,
    require_finish_time TEXT,
    first_complete_time TEXT,
    cancel_time TEXT,
    cancel_operator TEXT,
    cancel_reason TEXT,
    task_status TEXT,
    com_fault_speciality TEXT,
    com_fault_sub_speciality TEXT,
    com_fault_cause TEXT,
    leave_observations TEXT,
    site_id TEXT,
    site_priority TEXT,
    title TEXT,
    description TEXT,
    nro_toa TEXT,
    company_supply TEXT,
    zona TEXT,
    departamento TEXT,
    torrero TEXT,
    task_type TEXT,
    fm_office TEXT,
    region TEXT,
    site_id_name TEXT,
    site_location_type TEXT,
    sla TEXT,
    sla_status TEXT,
    suspend_state TEXT,
    create_operator TEXT,
    fault_first_occur_time TEXT,
    schedule_operator TEXT,
    schedule_time TEXT,
    assign_to_fme TEXT,
    assign_to_fme_full_name TEXT,
    dispatch_operator TEXT,
    depart_operator TEXT,
    arrive_operator TEXT,
    complete_operator TEXT,
    reject_time TEXT,
    reject_operator TEXT,
    reject_des TEXT,
    com_level_1_aff_equip TEXT,
    com_level_2_aff_equip TEXT,
    com_level_3_aff_equip TEXT,
    fault_type TEXT,
    task_category TEXT,
    task_subcategory TEXT,
    fme_contrator TEXT,
    contratista_sitio TEXT,
    complete_operator_name TEXT,
    reject_operator_name TEXT,
    arrive_operator_name TEXT,
    depart_operator_name TEXT,
    dispatch_operator_name TEXT,
    cancel_operator_name TEXT,
    schedule_operator_name TEXT,
    create_operator_name TEXT,
    situacion_encontrada TEXT,
    detalle_de_actuacion_realizada TEXT,
    atencion_incidencias TEXT,
    remedy_id TEXT,
    first_pause_time TEXT,
    first_pause_operator TEXT,
    first_pause_reason TEXT,
    first_resume_operator TEXT,
    first_resume_time TEXT,
    second_pause_time TEXT,
    second_pause_reason TEXT,
    second_pause_operator TEXT,
    second_resume_time TEXT,
    second_resume_operator TEXT,
    third_pause_time TEXT,
    third_pause_reason TEXT,
    third_pause_operator TEXT,
    third_resume_time TEXT,
    third_resume_operator TEXT,
    fourth_pause_time TEXT,
    fourth_pause_reason TEXT,
    fourth_pause_operator TEXT,
    fourth_resume_time TEXT,
    fourth_resume_operator TEXT,
    fifth_pause_time TEXT,
    fifth_pause_reason TEXT,
    fifth_pause_operator TEXT,
    fifth_resume_time TEXT,
    fifth_resume_operator TEXT,
    reject_flag TEXT,
    reject_counter TEXT,
    fuel_type TEXT
);


-- ============================================================================
-- 1) CF - BANCO DE BATERIAS
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.cf_banco_de_baterias (
    task_id                            VARCHAR(255) NOT NULL,
    site_id                            VARCHAR(255) NOT NULL,
    sub_wo_id                          VARCHAR(255) NOT NULL,

    tipo_de_banco_de_baterias          TEXT,
    servicio_que_respaldan             TEXT,
    cod_unico_eq_bat                   TEXT,
    cod_unico_cuadro_fza_rel_bb        TEXT,
    nom_cuadro_fza_asoc_banco_bat      TEXT,
    nom_sala_ubic_cuadro_fza           TEXT,
    numero_de_banco                    TEXT,
    cantidad_de_celdas                 TEXT,
    marca                              TEXT,
    modelo                             TEXT,
    estado_de_banco_de_baterias        TEXT,
    tiene_bornes_levantados            TEXT,
    tiene_alguna_celda_rajada          TEXT,
    capacidad_ah                       TEXT,
    fecha_de_instalacion               TEXT,
    cantidad_de_cables_por_polo        TEXT,
    calibre_de_cable                   TEXT,
    temperatura_numc                   TEXT,
    engrase                            TEXT,
    ajustes_reapriete_general          TEXT,
    limp_general_bornes_elem           TEXT,
    voltaje_de_flotacion               TEXT,
    corriente_de_recarga               TEXT,
    estado_bat_elem_anexos_puentes     TEXT,
    estado_bornes_cada_bat_celda       TEXT,
    estado_puentes_entre_cada_bat      TEXT,
    observ_que_bat_celdas_no           TEXT,
    medicion_de_impedancia             TEXT,
    autono_prueba_descar_contro        TEXT,
    observacion                        TEXT,
    fechacarga                         TIMESTAMP(0) NOT NULL
);

-- ============================================================================
-- 2) CF - BASTIDOR DISTRIBUCION
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.cf_bastidor_distribucion (
    task_id                         VARCHAR(255) NOT NULL,
    site_id                         VARCHAR(255) NOT NULL,
    sub_wo_id                       VARCHAR(255) NOT NULL,

    cuadro_fza_que_perten_coloca    TEXT,
    numero_unidad_distri            TEXT,
    limp_ajuste_pintad_bastid       TEXT,
    verif_fusibl_e_interr           TEXT,
    verif_cables_barras_interc      TEXT,
    calculo_de_caida_de_tension     TEXT,
    corriente_de_llegada_a          TEXT,
    voltaje_de_llegada_v            TEXT,
    temper_camara_termog_c          TEXT,
    limp_calibr_medido_consum       TEXT,
    comprobacion_de_alarmas         TEXT,
    actual_diagra_unifil            TEXT,
    rotula_bastid_distri_enumer     TEXT,
    observacion                     TEXT,
    fechacarga                      TIMESTAMP(0) NOT NULL
);

-- ============================================================================
-- 3) CF - CUADRO DE FUERZA
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.cf_cuadro_de_fuerza (
    task_id                             VARCHAR(255) NOT NULL,
    site_id                             VARCHAR(255) NOT NULL,
    sub_wo_id                           VARCHAR(255) NOT NULL,

    nom_cuadro_fza_ejempl_cabece        TEXT,
    nom_sala_ubic_cuadro_fza            TEXT,
    codigo_de_equipo                    TEXT,
    marca_de_cuadro_fuerza              TEXT,
    modelo_de_cuadro_de_fuerza          TEXT,
    modelo_de_unidad_rectificadora      TEXT,
    cuadro_fza_rotula_cod_unico_eq      TEXT,
    cantid_banco_bat_conect_cf          TEXT,
    capaci_total_banco_bat_conect       TEXT,
    consumo_real_de_cf_ampdc            TEXT,
    capaci_maxima_cuadro_fza            TEXT,
    capaci_actual_real_cuadro_fza       TEXT,
    cantidad_de_bastidores              TEXT,
    cantid_modulo_rectif                TEXT,
    anio_de_instalacion                 TEXT,
    estado                              TEXT,
    limpieza_y_o_cambio_de_filtros      TEXT,
    ajuste_de_terminales                TEXT,
    verif_alarma_extern                 TEXT,
    medici_corrie_unidad_baja_amp       TEXT,
    medici_voltaj_unidad_baja_v         TEXT,
    temper_camara_termog_pirome         TEXT,
    limp_ajuste_pintad_rotula_cada      TEXT,
    senali_peligr_acceso_restri         TEXT,
    estado_chapas_llaves_puerta         TEXT,
    limp_ajuste_pintad_barras           TEXT,
    verif_operac_redund                 TEXT,
    arranque_lento                      TEXT,
    alta_eficiencia                     TEXT,
    comparticion_de_carga               TEXT,
    limita_corrie_bat                   TEXT,
    limita_potenc_rectif                TEXT,
    compro_monito_local                 TEXT,
    actual_diagra_unifil                TEXT,
    regist_valor_ajuste_protec          TEXT,
    eq_rotula_cod_eq_nom_cuadro         TEXT,
    confir_correc_conex_cable           TEXT,
    nom_person_noc_energi_quien_se      TEXT,
    alarma_cableada_falla_de_red        TEXT,
    la_alarma_se_reflejo_en_el_noc      TEXT,
    alarma_cablea_grupo_electr          TEXT,
    alarma_se_reflej_noc_1              TEXT,
    alarma_cablea_falla_grupo           TEXT,
    alarma_se_reflej_noc_2              TEXT,
    alarma_cablea_alta_temper           TEXT,
    alarma_se_reflej_noc_3              TEXT,
    alarma_cablea_falla_rectif          TEXT,
    alarma_se_reflej_noc_4              TEXT,
    alarma_cablea_bajo_voltaj_bat       TEXT,
    alarma_se_reflej_noc_5              TEXT,
    alarma_cablea_bajo_nivel            TEXT,
    alarma_se_reflej_noc_6              TEXT,
    cuadro_fza_que_perten_modulo        TEXT,
    tipo_de_modulo_de_gestion           TEXT,
    marca_de_modulo_de_gestion          TEXT,
    modelo_de_modulo_de_gestion         TEXT,
    limp_ajuste_calibr_compon           TEXT,
    prueba_de_funcionamiento            TEXT,
    observacion                         TEXT,
    cod_prueba_alarma_noc               TEXT,
    fechacarga                          TIMESTAMP(0) NOT NULL
);

-- ============================================================================
-- 4) CF - MODULOS RECTIFICADORES
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.cf_modulos_rectificadores (
    task_id                        VARCHAR(255) NOT NULL,
    site_id                        VARCHAR(255) NOT NULL,
    sub_wo_id                      VARCHAR(255) NOT NULL,

    cuadro_fza_que_perten_coloca   TEXT,
    posici_modulo_rectif_cf        TEXT,
    marca                          TEXT,
    modelo                         TEXT,
    estado                         TEXT,
    voltaje_de_salida              TEXT,
    corrie_salida_solo_coloca      TEXT,
    contra_lectur_medici           TEXT,
    limp_polvo_soplet_rectif       TEXT,
    observacion                    TEXT,
    fechacarga                     TIMESTAMP(0) NOT NULL
);

-- ============================================================================
-- 5) CF - TABLERO AC DE CUADRO DE FU
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.cf_tablero_ac_de_cuadro_de_fu (
    task_id                           VARCHAR(255) NOT NULL,
    site_id                           VARCHAR(255) NOT NULL,
    sub_wo_id                         VARCHAR(255) NOT NULL,

    cuadro_fza_que_perten_coloca      TEXT,
    capacidad_de_itm_principal_a      TEXT,
    cantid_itm_no_inclui_interr       TEXT,
    consumo_en_interrupor_generala    TEXT,
    voltaje_en_interrupor_generala    TEXT,
    limp_pintad_tabler_corrie         TEXT,
    limp_ajuste_interr_conexi         TEXT,
    medici_verif_balanc_carga         TEXT,
    limpieza_y_calibracion_de_tvss    TEXT,
    limp_calibr_transf                TEXT,
    limp_calibr_shunt                 TEXT,
    limp_calibr_conexi_tierra         TEXT,
    medici_temper_camara_termog       TEXT,
    actual_diagra_unifil              TEXT,
    rotulacion_de_equipos             TEXT,
    observaciones                     TEXT,
    fechacarga                        TIMESTAMP(0) NOT NULL
);

-- ============================================================================
-- 6) CF - DESCARGA CONTROLADA BATERÍAS
--   (columna corregida: 45_min... -> min45_medir_voltaj_bb_que)
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.cf_descarga_controlada_bater (
    task_id                          VARCHAR(255) NOT NULL,
    site_id                          VARCHAR(255) NOT NULL,
    sub_wo_id                        VARCHAR(255) NOT NULL,

    equipo_conectado_a_baterias      TEXT,
    cod_unico_cuadro_fza_ups_asoc    TEXT,
    nom_cuadro_fza_ups_asoc          TEXT,
    carga_cuadro_fuerza_adc          TEXT,
    marca_de_cuadro_de_fuerza        TEXT,
    porcen_corrie_carga_bat          TEXT,
    capacidad_total_bb_teorica_ah    TEXT,
    cantidad_bancos_bat_asociados    TEXT,

    codigo_de_bat1                   TEXT,
    capacidad_bb1_ah                 TEXT,
    marca_bb1                        TEXT,
    anio_instalacion_bb1             TEXT,

    codigo_de_bat2                   TEXT,
    capacidad_bb2_ah                 TEXT,
    marca_bb2                        TEXT,
    anio_instalacion_bb2             TEXT,

    codigo_de_bat3                   TEXT,
    capacidad_bb3_ah                 TEXT,
    marca_bb3                        TEXT,
    anio_instalacion_bb3             TEXT,

    codigo_de_bat4                   TEXT,
    capacidad_bb4_ah                 TEXT,
    marca_bb4                        TEXT,
    anio_instalacion_bb4             TEXT,

    codigo_de_bat5                   TEXT,
    capacidad_bb5_ah                 TEXT,
    marca_bb5                        TEXT,
    anio_instalacion_bb5             TEXT,

    codigo_de_bat6                   TEXT,
    capacidad_bb6_ah                 TEXT,
    marca_bb6                        TEXT,
    anio_instalacion_bb6             TEXT,

    codigo_de_bat7                   TEXT,
    capacidad_bb7_ah                 TEXT,
    marca_bb7                        TEXT,
    anio_instalacion_bb7             TEXT,

    volt_rectif_a_los_15_min_vdc     TEXT,
    carga_asume_rectif_15_min_adc    TEXT,
    carga_asume_bb1_15_min_adc       TEXT,
    carga_asume_bb2_15_min_adc       TEXT,
    carga_asume_bb3_15_min_adc       TEXT,
    carga_asume_bb4_15_min_adc       TEXT,
    carga_asume_bb5_15_min_adc       TEXT,
    carga_asume_bb6_15_min_adc       TEXT,
    carga_asume_bb7_15_min_adc       TEXT,

    volt_rectif_a_los_30_min_vdc     TEXT,
    carga_asume_rectif_30_min_adc    TEXT,
    carga_asume_bb1_30_min_adc       TEXT,
    carga_asume_bb2_30_min_adc       TEXT,
    carga_asume_bb3_30_min_adc       TEXT,
    carga_asume_bb4_30_min_adc       TEXT,
    carga_asume_bb5_30_min_adc       TEXT,
    carga_asume_bb6_30_min_adc       TEXT,
    carga_asume_bb7_30_min_adc       TEXT,

    volt_rectif_a_los_45_min_vdc     TEXT,
    carga_asume_rectif_45_min_adc    TEXT,
    carga_asume_bb1_45_min_adc       TEXT,
    carga_asume_bb2_45_min_adc       TEXT,
    carga_asume_bb3_45_min_adc       TEXT,
    carga_asume_bb4_45_min_adc       TEXT,
    carga_asume_bb5_45_min_adc       TEXT,
    carga_asume_bb6_45_min_adc       TEXT,
    carga_asume_bb7_45_min_adc       TEXT,

    min45_medir_voltaj_bb_que        TEXT,
    cuadro_fza_tiene_autono          TEXT,
    hay_algun_bb_malas_condic_que    TEXT,
    bb_en_malas_condiciones          TEXT,
    hay_alguna_celda_cuyo_v_debajo   TEXT,
    indica_numero_celdas_malas       TEXT,
    observacion_general              TEXT,
    fechacarga                       TIMESTAMP(0) NOT NULL
);

-- ============================================================================
-- 7) IE - DATOS SPAT GENERAL
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.ie_datos_spat_general (
  task_id                        VARCHAR(255) NOT NULL,
  site_id                        VARCHAR(255) NOT NULL,
  sub_wo_id                      VARCHAR(255) NOT NULL,

  cantidad_de_pozos_total        TEXT,
  cantidad_de_mallas             TEXT,
  cantidad_de_pozos_unicos       TEXT,
  existe_spat_completo_para_mt   TEXT,
  pozos_varill_cables_barra      TEXT,
  report_robo_algun_compon       TEXT,
  mencionar_componentes_robados  TEXT,
  fechacarga                     TIMESTAMP(0) NOT NULL
);

-- ============================================================================
-- 8) IE - MANTENIMIENTO POZO POR POZO
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.ie_mantenimiento_pozo_por_poz (
  task_id                             VARCHAR(255) NOT NULL,
  site_id                             VARCHAR(255) NOT NULL,
  sub_wo_id                           VARCHAR(255) NOT NULL,

  pozo_interv_perten_una_malla        TEXT,
  nnum_pozo_a_intervenir              TEXT,
  nnum_malla_a_intervenir             TEXT,
  sistema_que_protege                 TEXT,
  sala_o_equipos_que_protege          TEXT,
  verif_contin_pozo_otro_pozo         TEXT,
  verif_contin_pozo_barra_equipo      TEXT,
  verif_resist_menor_5_ohmios_bt      TEXT,
  limp_ajuste_conect_engras           TEXT,
  pintado_y_senalizacion              TEXT,
  estado_regist_tapas_concre          TEXT,
  cambio_repara_tapas_concre          TEXT,
  debe_echars_agua_thorge_confir      TEXT,
  debe_echars_agua_thorge_confir_1    TEXT,
  lectur_resist_despue_echar          TEXT,
  observacion                         TEXT,
  fechacarga                          TIMESTAMP(0) NOT NULL
);

-- ============================================================================
-- 9) IE - SUMINISTRO DE ENERGIA
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.ie_suministro_de_energia (
  task_id                           VARCHAR(255) NOT NULL,
  site_id                           VARCHAR(255) NOT NULL,
  sub_wo_id                         VARCHAR(255) NOT NULL,

  fuente_de_energia                 TEXT,
  compania_electrica                TEXT,
  pertenencia_de_suministro         TEXT,
  tipo_de_tension_del_suministro    TEXT,
  propietario_del_suministro        TEXT,
  num_ro_de_suministro              TEXT,
  num_ro_de_medidor                 TEXT,
  num_ro_de_cliente                 TEXT,
  cantidad_de_fases                 TEXT,
  capacidad_de_itm_de_medidor_a     TEXT,
  calibr_cable_mm2_itm_tab          TEXT,
  calibr_cable_mm2_itm_conces       TEXT,
  voltaje_en_itm_v                  TEXT,
  distan_entre_sumini_estaci_m      TEXT,
  observacion                       TEXT,
  fechacarga                        TIMESTAMP(0) NOT NULL
);

-- ============================================================================
-- 10) IE - TABLERO PRINCIPAL
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.ie_tablero_principal (
  task_id                         VARCHAR(255) NOT NULL,
  site_id                         VARCHAR(255) NOT NULL,
  sub_wo_id                       VARCHAR(255) NOT NULL,

  nombre_de_tablero               TEXT,
  codigo_de_equipo                TEXT,
  cantidad_de_fases               TEXT,
  capacidad_itm_general_a         TEXT,
  voltaje_l1_l2_v                 TEXT,
  voltaje_l2_l3_v                 TEXT,
  voltaje_l3_l1_v                 TEXT,
  corriente_l1_a                  TEXT,
  corriente_l2_a                  TEXT,
  corriente_l3_a                  TEXT,
  corriente_n_a                   TEXT,
  cantidad_de_itm                 TEXT,
  limp_ajuste_tabler_termin       TEXT,
  repintado_de_areas_aisladas     TEXT,
  tipo_de_cable                   TEXT,
  calibre_de_cable                TEXT,
  tablero_aterrado                TEXT,
  medir_contin_entre_cable        TEXT,
  actual_diagra_unifil            TEXT,
  regist_valor_ajuste_protec      TEXT,
  rotulacion_de_equipos           TEXT,
  observacion                     TEXT,
  fechacarga                      TIMESTAMP(0) NOT NULL
);

-- ============================================================================
-- 11) IE - TABLERO SECUNDARIO
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.ie_tablero_secundario (
  task_id                          VARCHAR(255) NOT NULL,
  site_id                          VARCHAR(255) NOT NULL,
  sub_wo_id                        VARCHAR(255) NOT NULL,

  nombre_de_tablero                TEXT,
  capacidad_itm_general_a          TEXT,
  voltaje_l1_l2_v                  TEXT,
  voltaje_l2_l3_v                  TEXT,
  voltaje_l3_l1_v                  TEXT,
  corriente_l1_a                   TEXT,
  corriente_l2_a                   TEXT,
  corriente_l3_a                   TEXT,
  corriente_n_a                    TEXT,
  limp_ajuste_estruc_metali        TEXT,
  repintado_de_areas_aisladas      TEXT,
  tipo_de_cable                    TEXT,
  calibre_de_cable                 TEXT,
  aterramiento                     TEXT,
  observacion                      TEXT,
  actual_diagra_unifil             TEXT,
  regist_valor_ajuste_protec       TEXT,
  rotulacion_de_equipos            TEXT,
  tipo_propiedad                   TEXT,
  si_tabler_es_tercer_especi_nom   TEXT,
  fechacarga                       TIMESTAMP(0) NOT NULL
);

------public ------
CREATE TABLE public.error_pago_energia (
	nota_al_aprobador_char_40 varchar(255) NULL,
	concesionario varchar(255) NULL,
	ruc_concesionario varchar(255) NULL,
	numero_suministro varchar(255) NULL,
	recibo varchar(255) NULL,
	total_pagado varchar(255) NULL,
	fecha_emision varchar(255) NULL,
	fecha_vencimiento varchar(255) NULL,
	periodo_consumo varchar(255) NULL,
	archivo varchar(255) NULL,
	fecha_carga timestamptz NOT NULL,
	tabla_origen text NOT NULL
);
---ODS TABLE 
CREATE TABLE ods.sftp_hm_pago_energia (
	nota_al_aprobador_char_40 varchar(250) NULL,
	concesionario varchar(255) NULL,
	ruc_concesionario int8 NULL,
	numero_suministro varchar(255) NULL,
	recibo varchar(255) NOT NULL,
	total_pagado numeric(18, 2) NULL,
	fecha_emision date NULL,
	fecha_vencimiento date NULL,
	periodo_consumo varchar(255) NULL,
	archivo varchar(255) NULL,
	creation_user varchar(100) DEFAULT (((COALESCE(inet_client_addr()::text, 'local'::text) || '-'::text) || CURRENT_USER::text)) NULL,
	creation_date timestamp(0) DEFAULT clock_timestamp()::timestamp(0) without time zone NULL,
	creation_ip inet DEFAULT inet_client_addr() NULL,
	CONSTRAINT sftp_hm_pago_energia_pk PRIMARY KEY (recibo)
);
--------

CREATE TABLE ods.sftp_hd_toa (
	tecnico text NULL,
	id_recurso int4 NULL,
	nro_toa text NOT NULL,
	subtipo_de_actividad text NULL,
	numero_de_peticion text NULL,
	fecha_de_cita date NULL,
	sla_inicio timestamp NULL,
	sla_fin timestamp NULL,
	localidad text NULL,
	direccion text NULL,
	direccion_polar_x numeric NULL,
	direccion_polar_y numeric NULL,
	nombre_cliente text NULL,
	hora_de_asignacion_de_actividad timestamp NULL,
	fecha_de_registro_de_actividad_toa timestamp NULL,
	notas text NULL,
	codigo_de_cliente text NULL,
	fecha_hora_de_cancelacion timestamp NULL,
	empresa text NULL,
	bucket_inicial text NULL,
	usuario_iniciado text NULL,
	nombre_distrito text NULL,
	sistema_origen text NULL,
	id_del_ticket text NULL,
	quiebres text NULL,
	fecha_de_inicio_pint timestamp NULL,
	inicio_pr1 timestamp NULL,
	fin_pr1 timestamp NULL,
	fin_pr2 timestamp NULL,
	inicio_pr2 timestamp NULL,
	fin_pr3 timestamp NULL,
	inicio_pr3 timestamp NULL,
	fin_pr4 timestamp NULL,
	inicio_pr4 timestamp NULL,
	motivo_pr1 text NULL,
	motivo_pr2 text NULL,
	motivo_pr3 text NULL,
	motivo_pr4 text NULL,
	nombre_local text NULL,
	tipo_de_local text NULL,
	zona_geografica text NULL,
	zona text NULL,
	estado_toa text NULL,
	duracion_pr1 text NULL,
	duracion_pr2 text NULL,
	duracion_pr3 text NULL,
	duracion_pr4 text NULL,
	estado text NULL,
	codigo_completado_reparacion_y_preventivo_cable text NULL,
	cliente_acepta_solucion_anticipada_de_reclamo text NULL,
	averia_efectiva_marca_sar_t text NULL,
	creation_user varchar(100) DEFAULT (((COALESCE(inet_client_addr()::text, 'local'::text) || '-'::text) || CURRENT_USER::text)) NULL,
	creation_date timestamp(0) DEFAULT clock_timestamp()::timestamp(0) without time zone NULL,
	creation_ip inet DEFAULT inet_client_addr() NULL,
	CONSTRAINT pk_sftp_hd_toa PRIMARY KEY (nro_toa)
);


----

CREATE TABLE IF NOT EXISTS ods.web_hm_autin_infogeneral (
    task_id TEXT,
    createtime timestamp,
    dispatch_time timestamp,
    accept_time timestamp,
    depart_time timestamp,
    arrive_time timestamp,
    complete_time timestamp,
    require_finish_time TEXT,
    first_complete_time timestamp,
    cancel_time timestamp,
    cancel_operator TEXT,
    cancel_reason TEXT,
    task_status TEXT,
    com_fault_speciality TEXT,
    com_fault_sub_speciality TEXT,
    com_fault_cause TEXT,
    leave_observations TEXT,
    site_id TEXT,
    site_priority TEXT,
    title TEXT,
    description TEXT,
    nro_toa TEXT,
    company_supply TEXT,
    zona TEXT,
    departamento TEXT,
    torrero TEXT,
    task_type TEXT,
    fm_office TEXT,
    region text,
    site_id_name TEXT,
    site_location_type TEXT,
    sla int8 NULL,
    sla_status TEXT,
    suspend_state TEXT,
    create_operator TEXT,
    fault_first_occur_time timestamp,
    schedule_operator TEXT,
    schedule_time timestamp,
    assign_to_fme TEXT,
    assign_to_fme_full_name TEXT,
    dispatch_operator TEXT,
    depart_operator TEXT,
    arrive_operator TEXT,
    complete_operator TEXT,
    reject_time TEXT,
    reject_operator TEXT,
    reject_des TEXT,
    com_level_1_aff_equip TEXT,
    com_level_2_aff_equip TEXT,
    com_level_3_aff_equip TEXT,
    fault_type TEXT,
    task_category TEXT,
    task_subcategory TEXT,
    fme_contrator TEXT,
    contratista_sitio TEXT,
    complete_operator_name TEXT,
    reject_operator_name text,
    arrive_operator_name text,
    depart_operator_name TEXT,
    dispatch_operator_name TEXT,
    cancel_operator_name TEXT,
    schedule_operator_name TEXT,
    create_operator_name TEXT,
    situacion_encontrada TEXT,
    detalle_de_actuacion_realizada TEXT,
    atencion_incidencias TEXT,
    remedy_id TEXT,
    first_pause_time timestamp,
    first_pause_operator TEXT,
    first_pause_reason TEXT,
    first_resume_operator TEXT,
    first_resume_time timestamp,
    second_pause_time timestamp,
    second_pause_reason TEXT,
    second_pause_operator TEXT,
    second_resume_time timestamp,
    second_resume_operator TEXT,
    third_pause_time timestamp,
    third_pause_reason TEXT,
    third_pause_operator TEXT,
    third_resume_time timestamp,
    third_resume_operator TEXT,
    fourth_pause_time timestamp,
    fourth_pause_reason TEXT,
    fourth_pause_operator TEXT,
    fourth_resume_time timestamp,
    fourth_resume_operator TEXT,
    fifth_pause_time timestamp,
    fifth_pause_reason text,
    fifth_pause_operator TEXT,
    fifth_resume_time timestamp,
    fifth_resume_operator TEXT,
    reject_flag TEXT,
    reject_counter int8 NULL,
    fuel_type text,
    creation_user varchar(100) DEFAULT ((COALESCE(inet_client_addr()::text, 'local') || '-' || CURRENT_USER::text)),
    creation_date timestamp(0) DEFAULT clock_timestamp()::timestamp(0),
    creation_ip inet DEFAULT inet_client_addr(),
    CONSTRAINT liq_task_id_de_luz_pk PRIMARY KEY (task_id)
);

-----funcion ods

CREATE OR REPLACE FUNCTION public.error_pago_energia_ultimo_lote(p_tabla text)
 RETURNS SETOF error_pago_energia
 LANGUAGE sql
 STABLE
AS $function$
  SELECT e.*
  FROM public.error_pago_energia e
  WHERE lower(btrim(e.tabla_origen)) = lower(btrim(p_tabla))
    AND e.fecha_carga::date = CURRENT_DATE
    AND e.fecha_carga = (
      SELECT MAX(fecha_carga)
      FROM public.error_pago_energia
      WHERE lower(btrim(tabla_origen)) = lower(btrim(p_tabla))
        AND fecha_carga::date = CURRENT_DATE
    );
$function$
;
----------
CREATE OR REPLACE PROCEDURE ods.sp_cargar_sftp_hd_toa()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio    timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp     integer      := 'ods.sp_cargar_sftp_hd_toa()'::regprocedure::oid::int;
  v_sp_name   text         := 'ods.sp_cargar_sftp_hd_toa()'::regprocedure::text;

  v_inserted  integer := 0;
  v_updated   integer := 0;
  v_deleted   integer := 0;
  v_nulls     integer := 0;

  v_estado    varchar(50);
  v_msj       text;
BEGIN
  -- 1) Contamos filas actuales en destino y filas sin PK en origen
  SELECT COUNT(*) INTO v_deleted
  FROM ods.sftp_hd_toa;

  SELECT COUNT(*) INTO v_nulls
  FROM raw.sftp_hd_toa r
  WHERE r.nro_toa IS NULL OR btrim(r.nro_toa) = '';

  -- 2) Full refresh de la tabla destino
  TRUNCATE TABLE ods.sftp_hd_toa;

  WITH raw_clean AS (
    SELECT
      NULLIF(btrim(r.tecnico), '') AS tecnico,

      CASE
        WHEN btrim(r.id_recurso) ~ '^[0-9]+$'
          THEN btrim(r.id_recurso)::int
        ELSE NULL
      END AS id_recurso,

      NULLIF(btrim(r.nro_toa), '') AS nro_toa,
      NULLIF(btrim(r.subtipo_de_actividad), '') AS subtipo_de_actividad,
      NULLIF(btrim(r.numero_de_peticion), '') AS numero_de_peticion,

      /* ===== fecha_de_cita (DATE) ===== */
      CASE
        WHEN NULLIF(btrim(r.fecha_de_cita), '') IS NULL THEN NULL
        WHEN btrim(r.fecha_de_cita) ~ '^\d{4}-\d{2}-\d{2}$'
          THEN btrim(r.fecha_de_cita)::date
        WHEN btrim(r.fecha_de_cita) ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(btrim(r.fecha_de_cita), 'DD/MM/YYYY')
        WHEN btrim(r.fecha_de_cita) ~ '^\d{2}/\d{2}/\d{2}$'
          THEN to_date(btrim(r.fecha_de_cita), 'DD/MM/YY')
        ELSE NULL
      END AS fecha_de_cita,

      /* ===== TIMESTAMP genéricos (distintos formatos) ===== */

      -- sla_inicio
      CASE
        WHEN NULLIF(btrim(r.sla_inicio), '') IS NULL THEN NULL
        WHEN btrim(r.sla_inicio) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.sla_inicio)::timestamp
        WHEN btrim(r.sla_inicio) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.sla_inicio), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.sla_inicio) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.sla_inicio)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS sla_inicio,

      -- sla_fin
      CASE
        WHEN NULLIF(btrim(r.sla_fin), '') IS NULL THEN NULL
        WHEN btrim(r.sla_fin) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.sla_fin)::timestamp
        WHEN btrim(r.sla_fin) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.sla_fin), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.sla_fin) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.sla_fin)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS sla_fin,

      NULLIF(btrim(r.localidad), '') AS localidad,
      NULLIF(btrim(r.direccion), '') AS direccion,

      -- direccion_polar_x (NUMERIC)
      CASE
        WHEN NULLIF(btrim(r.direccion_polar_x), '') IS NULL THEN NULL
        WHEN btrim(r.direccion_polar_x) ~ '^[+-]?[0-9]+([.,][0-9]+)?$'
          THEN REPLACE(btrim(r.direccion_polar_x), ',', '.')::numeric
        ELSE NULL
      END AS direccion_polar_x,

      -- direccion_polar_y (NUMERIC)
      CASE
        WHEN NULLIF(btrim(r.direccion_polar_y), '') IS NULL THEN NULL
        WHEN btrim(r.direccion_polar_y) ~ '^[+-]?[0-9]+([.,][0-9]+)?$'
          THEN REPLACE(btrim(r.direccion_polar_y), ',', '.')::numeric
        ELSE NULL
      END AS direccion_polar_y,

      NULLIF(btrim(r.nombre_cliente), '') AS nombre_cliente,

      -- hora_de_asignacion_de_actividad
      CASE
        WHEN NULLIF(btrim(r.hora_de_asignacion_de_actividad), '') IS NULL THEN NULL
        WHEN btrim(r.hora_de_asignacion_de_actividad) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.hora_de_asignacion_de_actividad)::timestamp
        WHEN btrim(r.hora_de_asignacion_de_actividad) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.hora_de_asignacion_de_actividad), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.hora_de_asignacion_de_actividad) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.hora_de_asignacion_de_actividad)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS hora_de_asignacion_de_actividad,

      -- fecha_de_registro_de_actividad_toa
      CASE
        WHEN NULLIF(btrim(r.fecha_de_registro_de_actividad_toa), '') IS NULL THEN NULL
        WHEN btrim(r.fecha_de_registro_de_actividad_toa) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fecha_de_registro_de_actividad_toa)::timestamp
        WHEN btrim(r.fecha_de_registro_de_actividad_toa) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fecha_de_registro_de_actividad_toa), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fecha_de_registro_de_actividad_toa) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fecha_de_registro_de_actividad_toa)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fecha_de_registro_de_actividad_toa,

      NULLIF(btrim(r.notas), '') AS notas,
      NULLIF(btrim(r.codigo_de_cliente), '') AS codigo_de_cliente,

      -- fecha_hora_de_cancelacion
      CASE
        WHEN NULLIF(btrim(r.fecha_hora_de_cancelacion), '') IS NULL THEN NULL
        WHEN btrim(r.fecha_hora_de_cancelacion) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fecha_hora_de_cancelacion)::timestamp
        WHEN btrim(r.fecha_hora_de_cancelacion) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fecha_hora_de_cancelacion), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fecha_hora_de_cancelacion) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fecha_hora_de_cancelacion)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fecha_hora_de_cancelacion,

      NULLIF(btrim(r.empresa), '') AS empresa,
      NULLIF(btrim(r.bucket_inicial), '') AS bucket_inicial,
      NULLIF(btrim(r.usuario_iniciado), '') AS usuario_iniciado,
      NULLIF(btrim(r.nombre_distrito), '') AS nombre_distrito,
      NULLIF(btrim(r.sistema_origen), '') AS sistema_origen,
      NULLIF(btrim(r.id_del_ticket), '') AS id_del_ticket,
      NULLIF(btrim(r.quiebres), '') AS quiebres,

      -- fecha_de_inicio_pint
      CASE
        WHEN NULLIF(btrim(r.fecha_de_inicio_pint), '') IS NULL THEN NULL
        WHEN btrim(r.fecha_de_inicio_pint) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fecha_de_inicio_pint)::timestamp
        WHEN btrim(r.fecha_de_inicio_pint) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fecha_de_inicio_pint), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fecha_de_inicio_pint) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fecha_de_inicio_pint)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fecha_de_inicio_pint,

      -- inicio_pr1
      CASE
        WHEN NULLIF(btrim(r.inicio_pr1), '') IS NULL THEN NULL
        WHEN btrim(r.inicio_pr1) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.inicio_pr1)::timestamp
        WHEN btrim(r.inicio_pr1) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.inicio_pr1), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.inicio_pr1) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.inicio_pr1)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS inicio_pr1,

      -- fin_pr1
      CASE
        WHEN NULLIF(btrim(r.fin_pr1), '') IS NULL THEN NULL
        WHEN btrim(r.fin_pr1) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fin_pr1)::timestamp
        WHEN btrim(r.fin_pr1) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fin_pr1), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fin_pr1) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fin_pr1)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fin_pr1,

      -- fin_pr2
      CASE
        WHEN NULLIF(btrim(r.fin_pr2), '') IS NULL THEN NULL
        WHEN btrim(r.fin_pr2) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fin_pr2)::timestamp
        WHEN btrim(r.fin_pr2) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fin_pr2), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fin_pr2) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fin_pr2)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fin_pr2,

      -- inicio_pr2
      CASE
        WHEN NULLIF(btrim(r.inicio_pr2), '') IS NULL THEN NULL
        WHEN btrim(r.inicio_pr2) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.inicio_pr2)::timestamp
        WHEN btrim(r.inicio_pr2) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.inicio_pr2), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.inicio_pr2) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.inicio_pr2)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS inicio_pr2,

      -- fin_pr3
      CASE
        WHEN NULLIF(btrim(r.fin_pr3), '') IS NULL THEN NULL
        WHEN btrim(r.fin_pr3) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fin_pr3)::timestamp
        WHEN btrim(r.fin_pr3) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fin_pr3), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fin_pr3) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fin_pr3)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fin_pr3,

      -- inicio_pr3
      CASE
        WHEN NULLIF(btrim(r.inicio_pr3), '') IS NULL THEN NULL
        WHEN btrim(r.inicio_pr3) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.inicio_pr3)::timestamp
        WHEN btrim(r.inicio_pr3) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.inicio_pr3), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.inicio_pr3) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.inicio_pr3)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS inicio_pr3,

      -- fin_pr4
      CASE
        WHEN NULLIF(btrim(r.fin_pr4), '') IS NULL THEN NULL
        WHEN btrim(r.fin_pr4) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fin_pr4)::timestamp
        WHEN btrim(r.fin_pr4) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fin_pr4), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fin_pr4) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fin_pr4)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fin_pr4,

      -- inicio_pr4
      CASE
        WHEN NULLIF(btrim(r.inicio_pr4), '') IS NULL THEN NULL
        WHEN btrim(r.inicio_pr4) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.inicio_pr4)::timestamp
        WHEN btrim(r.inicio_pr4) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.inicio_pr4), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.inicio_pr4) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.inicio_pr4)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS inicio_pr4,

      NULLIF(btrim(r.motivo_pr1), '') AS motivo_pr1,
      NULLIF(btrim(r.motivo_pr2), '') AS motivo_pr2,
      NULLIF(btrim(r.motivo_pr3), '') AS motivo_pr3,
      NULLIF(btrim(r.motivo_pr4), '') AS motivo_pr4,
      NULLIF(btrim(r.nombre_local), '') AS nombre_local,
      NULLIF(btrim(r.tipo_de_local), '') AS tipo_de_local,
      NULLIF(btrim(r.zona_geografica), '') AS zona_geografica,
      NULLIF(btrim(r.zona), '') AS zona,
      NULLIF(btrim(r.estado_toa), '') AS estado_toa,
      NULLIF(btrim(r.duracion_pr1), '') AS duracion_pr1,
      NULLIF(btrim(r.duracion_pr2), '') AS duracion_pr2,
      NULLIF(btrim(r.duracion_pr3), '') AS duracion_pr3,
      NULLIF(btrim(r.duracion_pr4), '') AS duracion_pr4,
      NULLIF(btrim(r.estado), '') AS estado,
      NULLIF(btrim(r.codigo_completado_reparacion_y_preventivo_cable), '') AS codigo_completado_reparacion_y_preventivo_cable,
      NULLIF(btrim(r.cliente_acepta_solucion_anticipada_de_reclamo), '') AS cliente_acepta_solucion_anticipada_de_reclamo,
      NULLIF(btrim(r.averia_efectiva_marca_sar_t), '') AS averia_efectiva_marca_sar_t
    FROM raw.sftp_hd_toa r
  ),
  dedup AS (
    SELECT *
    FROM (
      SELECT
        rc.*,
        ROW_NUMBER() OVER (
          PARTITION BY rc.nro_toa
          ORDER BY rc.fecha_de_registro_de_actividad_toa DESC NULLS LAST
        ) AS rn
      FROM raw_clean rc
    ) t
    WHERE rn = 1
  ),
  ins AS (
    INSERT INTO ods.sftp_hd_toa (
      tecnico,
      id_recurso,
      nro_toa,
      subtipo_de_actividad,
      numero_de_peticion,
      fecha_de_cita,
      sla_inicio,
      sla_fin,
      localidad,
      direccion,
      direccion_polar_x,
      direccion_polar_y,
      nombre_cliente,
      hora_de_asignacion_de_actividad,
      fecha_de_registro_de_actividad_toa,
      notas,
      codigo_de_cliente,
      fecha_hora_de_cancelacion,
      empresa,
      bucket_inicial,
      usuario_iniciado,
      nombre_distrito,
      sistema_origen,
      id_del_ticket,
      quiebres,
      fecha_de_inicio_pint,
      inicio_pr1,
      fin_pr1,
      fin_pr2,
      inicio_pr2,
      fin_pr3,
      inicio_pr3,
      fin_pr4,
      inicio_pr4,
      motivo_pr1,
      motivo_pr2,
      motivo_pr3,
      motivo_pr4,
      nombre_local,
      tipo_de_local,
      zona_geografica,
      zona,
      estado_toa,
      duracion_pr1,
      duracion_pr2,
      duracion_pr3,
      duracion_pr4,
      estado,
      codigo_completado_reparacion_y_preventivo_cable,
      cliente_acepta_solucion_anticipada_de_reclamo,
      averia_efectiva_marca_sar_t
    )
    SELECT
      tecnico,
      id_recurso,
      nro_toa,
      subtipo_de_actividad,
      numero_de_peticion,
      fecha_de_cita,
      sla_inicio,
      sla_fin,
      localidad,
      direccion,
      direccion_polar_x,
      direccion_polar_y,
      nombre_cliente,
      hora_de_asignacion_de_actividad,
      fecha_de_registro_de_actividad_toa,
      notas,
      codigo_de_cliente,
      fecha_hora_de_cancelacion,
      empresa,
      bucket_inicial,
      usuario_iniciado,
      nombre_distrito,
      sistema_origen,
      id_del_ticket,
      quiebres,
      fecha_de_inicio_pint,
      inicio_pr1,
      fin_pr1,
      fin_pr2,
      inicio_pr2,
      fin_pr3,
      inicio_pr3,
      fin_pr4,
      inicio_pr4,
      motivo_pr1,
      motivo_pr2,
      motivo_pr3,
      motivo_pr4,
      nombre_local,
      tipo_de_local,
      zona_geografica,
      zona,
      estado_toa,
      duracion_pr1,
      duracion_pr2,
      duracion_pr3,
      duracion_pr4,
      estado,
      codigo_completado_reparacion_y_preventivo_cable,
      cliente_acepta_solucion_anticipada_de_reclamo,
      averia_efectiva_marca_sar_t
    FROM dedup
    WHERE nro_toa IS NOT NULL
    RETURNING 1
  )
  SELECT COALESCE((SELECT COUNT(*) FROM ins), 0)
  INTO v_inserted;

  v_estado := 'DONE';
  v_msj := format(
    'Insert ods.sftp_hd_toa: inserted=%s, deleted=%s, null_nro_toa=%s',
    v_inserted, v_deleted, v_nulls
  );

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
    v_estado := 'ERROR';
    v_msj := SQLERRM;
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
    RAISE;
END;
$procedure$
;



----------

-- DROP PROCEDURE ods.sp_cargar_sftp_hm_pago_energia();

CREATE OR REPLACE PROCEDURE ods.sp_cargar_sftp_hm_pago_energia()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio        timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp         integer      := 'ods.sp_cargar_sftp_hm_pago_energia()'::regprocedure::oid::int;
  v_sp_name       text         := 'ods.sp_cargar_sftp_hm_pago_energia()'::regprocedure::text;

  v_inserted      integer := 0;
  v_updated       integer := 0;
  v_err_dups      integer := 0;
  v_err_nulos     integer := 0;
  v_err_total     integer := 0;

  v_estado        varchar(50);
  v_msj           text;
BEGIN
  /* ========= 1.a) NULOS / VACÍOS en RECIBO ========= */
  WITH cand AS (
    SELECT r.*
    FROM raw.sftp_mm_pago_energia r
    WHERE r.recibo IS NULL
       OR btrim(r.recibo) = ''
       OR lower(btrim(r.recibo)) IN ('nan','null')
  )
  INSERT INTO public.error_pago_energia (
    nota_al_aprobador_char_40,
    concesionario,
    ruc_concesionario,
    numero_suministro,
    recibo,
    total_pagado,
    fecha_emision,
    fecha_vencimiento,
    periodo_consumo,
    archivo,
    fecha_carga,
    tabla_origen
  )
  SELECT
    r.nota_al_aprobador_char_40,
    r.concesionario,
    r.ruc_concesionario,
    r.numero_suministro,
    r.recibo,
    r.total_pagado,
    r.fecha_emision,
    r.fecha_vencimiento,
    r.periodo_consumo,
    r.archivo,
    clock_timestamp(),
    'raw.sftp_mm_pago_energia'
  FROM cand r
  LEFT JOIN public.error_pago_energia e
         ON ( (e.recibo IS NULL AND r.recibo IS NULL) OR e.recibo = r.recibo )
        AND e.tabla_origen = 'raw.sftp_mm_pago_energia'
        AND e.fecha_carga::date = clock_timestamp()::date
  WHERE e.fecha_carga IS NULL;

  GET DIAGNOSTICS v_err_nulos = ROW_COUNT;

  -- Borro NULOS/VACÍOS de RAW
  WITH rid_nulos AS (
    SELECT r.ctid AS rid
    FROM raw.sftp_mm_pago_energia r
    WHERE r.recibo IS NULL
       OR btrim(r.recibo) = ''
       OR lower(btrim(r.recibo)) IN ('nan','null')
  )
  DELETE FROM raw.sftp_mm_pago_energia r
  USING rid_nulos x
  WHERE r.ctid = x.rid;

  /* ========= 1.b) DUPLICADOS EXACTOS por RECIBO ========= */
  WITH claves_dup AS (
    SELECT r.recibo
    FROM raw.sftp_mm_pago_energia r
    GROUP BY r.recibo
    HAVING COUNT(*) > 1
  ),
  cand_dups AS (
    SELECT r.*
    FROM raw.sftp_mm_pago_energia r
    JOIN claves_dup d ON d.recibo = r.recibo
  )
  INSERT INTO public.error_pago_energia (
    nota_al_aprobador_char_40,
    concesionario,
    ruc_concesionario,
    numero_suministro,
    recibo,
    total_pagado,
    fecha_emision,
    fecha_vencimiento,
    periodo_consumo,
    archivo,
    fecha_carga,
    tabla_origen
  )
  SELECT
    r.nota_al_aprobador_char_40,
    r.concesionario,
    r.ruc_concesionario,
    r.numero_suministro,
    r.recibo,
    r.total_pagado,
    r.fecha_emision,
    r.fecha_vencimiento,
    r.periodo_consumo,
    r.archivo,
    clock_timestamp(),
    'raw.sftp_mm_pago_energia'
  FROM cand_dups r
  LEFT JOIN public.error_pago_energia e
         ON e.recibo = r.recibo
        AND e.tabla_origen = 'raw.sftp_mm_pago_energia'
        AND e.fecha_carga::date = clock_timestamp()::date
  WHERE e.recibo IS NULL;

  GET DIAGNOSTICS v_err_dups = ROW_COUNT;

  -- Borro duplicados por RECIBO de RAW (se envían sólo a error)
  WITH claves_dup AS (
    SELECT r.recibo
    FROM raw.sftp_mm_pago_energia r
    GROUP BY r.recibo
    HAVING COUNT(*) > 1
  ),
  rid_dup AS (
    SELECT r.ctid AS rid
    FROM raw.sftp_mm_pago_energia r
    JOIN claves_dup d ON d.recibo = r.recibo
  )
  DELETE FROM raw.sftp_mm_pago_energia r
  USING rid_dup x
  WHERE r.ctid = x.rid;

  v_err_total := COALESCE(v_err_dups,0) + COALESCE(v_err_nulos,0);

  /* ========= 2) TRANSFORMACIÓN + UPSERT A ODS ========= */
  CREATE TEMP TABLE tmp_sftp_pago_energia_dedup
  ON COMMIT DROP AS
  WITH base AS (
    SELECT
      r.ctid AS rid,

      NULLIF(NULLIF(btrim(r.nota_al_aprobador_char_40),'NaN'),'')::varchar(250)
        AS nota_al_aprobador_char_40,
      NULLIF(NULLIF(btrim(r.concesionario),'NaN'),'')::varchar(255)
        AS concesionario,
      trunc(public.try_numeric(r.ruc_concesionario))::bigint
        AS ruc_concesionario,
      NULLIF(NULLIF(btrim(r.numero_suministro),'NaN'),'')::varchar(255)
        AS numero_suministro,

      CASE
        WHEN r.recibo IS NULL
          OR btrim(r.recibo) = ''
          OR lower(btrim(r.recibo)) IN ('nan','null')
        THEN NULL
        ELSE left(btrim(r.recibo), 255)
      END::varchar(255) AS recibo,

      public.try_numeric(r.total_pagado)::numeric(18,2)
        AS total_pagado,

      CASE
        WHEN r.fecha_emision IS NULL
          OR btrim(r.fecha_emision) = ''
          OR lower(btrim(r.fecha_emision)) IN ('nan','null')
        THEN NULL
        WHEN position('/' in r.fecha_emision) > 0
          THEN to_date(split_part(r.fecha_emision,' ',1), 'DD/MM/YYYY')
        ELSE to_date(split_part(r.fecha_emision,' ',1), 'YYYY-MM-DD')
      END AS fecha_emision,

      CASE
        WHEN r.fecha_vencimiento IS NULL
          OR btrim(r.fecha_vencimiento) = ''
          OR lower(btrim(r.fecha_vencimiento)) IN ('nan','null')
        THEN NULL
        WHEN position('/' in r.fecha_vencimiento) > 0
          THEN to_date(split_part(r.fecha_vencimiento,' ',1), 'DD/MM/YYYY')
        ELSE to_date(split_part(r.fecha_vencimiento,' ',1), 'YYYY-MM-DD')
      END AS fecha_vencimiento,

      NULLIF(NULLIF(btrim(r.periodo_consumo),'NaN'),'')::varchar(255)
        AS periodo_consumo,

      NULLIF(NULLIF(btrim(r.archivo),'NaN'),'')::varchar(255)
        AS archivo

    FROM raw.sftp_mm_pago_energia r
  ),
  dedup AS (
    -- PK lógica ODS: recibo (conserva primera aparición)
    SELECT DISTINCT ON (recibo) *
    FROM base
    WHERE recibo IS NOT NULL
    ORDER BY recibo, rid
  )
  SELECT
    nota_al_aprobador_char_40,
    concesionario,
    ruc_concesionario,
    numero_suministro,
    recibo,
    total_pagado,
    fecha_emision,
    fecha_vencimiento,
    periodo_consumo,
    archivo
  FROM dedup;

  -- ========= 2.a) UPDATE (parte UPDATE del UPSERT) =========
  UPDATE ods.sftp_hm_pago_energia o
  SET
    nota_al_aprobador_char_40 = t.nota_al_aprobador_char_40,
    concesionario             = t.concescionario,
    ruc_concesionario         = t.ruc_concesionario,
    numero_suministro         = t.numero_suministro,
    total_pagado              = t.total_pagado,
    fecha_emision             = t.fecha_emision,
    fecha_vencimiento         = t.fecha_vencimiento,
    periodo_consumo           = t.periodo_consumo,
    archivo                   = t.archivo
  FROM tmp_sftp_pago_energia_dedup t
  WHERE o.recibo = t.recibo;

  GET DIAGNOSTICS v_updated = ROW_COUNT;

  -- ========= 2.b) INSERT (parte INSERT del UPSERT) =========
  INSERT INTO ods.sftp_hm_pago_energia (
    nota_al_aprobador_char_40,
    concesionario,
    ruc_concesionario,
    numero_suministro,
    recibo,
    total_pagado,
    fecha_emision,
    fecha_vencimiento,
    periodo_consumo,
    archivo
  )
  SELECT
    t.nota_al_aprobador_char_40,
    t.concesionario,
    t.ruc_concesionario,
    t.numero_suministro,
    t.recibo,
    t.total_pagado,
    t.fecha_emision,
    t.fecha_vencimiento,
    t.periodo_consumo,
    t.archivo
  FROM tmp_sftp_pago_energia_dedup t
  LEFT JOIN ods.sftp_hm_pago_energia o
    ON o.recibo = t.recibo
  WHERE o.recibo IS NULL;

  GET DIAGNOSTICS v_inserted = ROW_COUNT;

  /* ========= 3) LOG ========= */
  v_estado := 'DONE';
  v_msj := format(
    'UPSERT en ODS.sftp_hm_pago_energia -> Insertados: %s | Actualizados: %s | Enviados a public.error_pago_energia (hoy): %s (duplicados recibo: %s, nulos/vacíos recibo: %s) | Origen: raw.sftp_mm_pago_energia.',
    COALESCE(v_inserted,0),
    COALESCE(v_updated,0),
    COALESCE(v_err_total,0),
    COALESCE(v_err_dups,0),
    COALESCE(v_err_nulos,0)
  );

  CALL public.sp_grabar_log_sp(
    p_id_sp      => v_id_sp,
    p_inicio     => v_inicio,
    p_fin        => clock_timestamp()::timestamp(0),
    p_inserted   => v_inserted,
    p_updated    => v_updated,
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




-----
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
    'raw.ie_tablero_secundario'

    -- GE / RADIO / SE (nuevas)
    --'raw.ge_grupo_electrogeno',
    --'raw.ge_info_general_de_tanque',
    --'raw.ge_limp_interna_tk',
    --'raw.ge_tablero_de_transferencia_a',
    --'raw.ge_transferencia_con_carga',
    --'raw.radio_6_12_18_bas_cf_bb',
    --'raw.radio_6_12_18_bbu',
    --'raw.se_banco_de_condensadores',
    --'raw.se_proteccion_y_pararrayos',
    --'raw.se_tablero_de_paso_de_salida',
    --'raw.se_trafomix',
    --'raw.se_transformador_de_potencia'
  ];

  v_t        text;
  v_keep_ts  timestamp;
  v_borradas bigint;
  v_detalle  text := '';
BEGIN
  FOREACH v_t IN ARRAY v_tablas LOOP
    BEGIN
      EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %s (fechacarga)',
        replace(v_t, '.', '_') || '_fechacarga_idx', v_t
      );

      EXECUTE format(
        'SELECT MAX(fechacarga) FROM %s WHERE fechacarga::date = $1', v_t
      ) USING p_fecha INTO v_keep_ts;

      IF v_keep_ts IS NULL THEN
        v_detalle := v_detalle || format('%s: sin filas %s; ', v_t, p_fecha);
        CONTINUE;
      END IF;

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
$procedure$
;


------


CREATE OR REPLACE PROCEDURE ods.sp_cargar_web_hm_autin_infogeneral()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio   timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp    integer      := 'ods.sp_cargar_web_hm_autin_infogeneral()'::regprocedure::oid::int;
  v_sp_name  text         := 'ods.sp_cargar_web_hm_autin_infogeneral()';
  v_inserted integer := 0;
  v_updated  integer := 0;
  v_deleted  integer := 0;  -- no borra
  v_nulls    integer := NULL;
  v_estado   varchar(50);
  v_msj      text;
BEGIN
  /*
    1) STG: limpia y castea tipos desde RAW.
    2) DEDUP: toma 1 fila por task_id (DISTINCT ON).
    3) UPD: actualiza registros existentes en ODS.
    4) INS: inserta solo los task_id que NO existen en ODS.
  */
  WITH
  stg AS (
    SELECT
      NULLIF(btrim(r.task_id), '') AS task_id,

      /* --------- Timestamp helpers (varios formatos) --------- */
      -- createtime
      CASE
        WHEN NULLIF(btrim(r.createtime),'') IS NULL THEN NULL
        WHEN regexp_replace(r.createtime,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.createtime,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.createtime ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.createtime::date::timestamp
        WHEN r.createtime ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.createtime,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.createtime ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.createtime,'DD/MM/YYYY HH24:MI')
        WHEN r.createtime ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.createtime,'DD/MM/YYYY')::timestamp
        WHEN r.createtime ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.createtime,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.createtime ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.createtime,'DD-MM-YYYY HH24:MI')
        WHEN r.createtime ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.createtime,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS createtime,

      -- dispatch_time
      CASE
        WHEN NULLIF(btrim(r.dispatch_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.dispatch_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.dispatch_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.dispatch_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.dispatch_time::date::timestamp
        WHEN r.dispatch_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.dispatch_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.dispatch_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.dispatch_time,'DD/MM/YYYY HH24:MI')
        WHEN r.dispatch_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.dispatch_time,'DD/MM/YYYY')::timestamp
        WHEN r.dispatch_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.dispatch_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.dispatch_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.dispatch_time,'DD-MM-YYYY HH24:MI')
        WHEN r.dispatch_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.dispatch_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS dispatch_time,

      -- accept_time
      CASE
        WHEN NULLIF(btrim(r.accept_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.accept_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.accept_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.accept_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.accept_time::date::timestamp
        WHEN r.accept_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.accept_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.accept_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.accept_time,'DD/MM/YYYY HH24:MI')
        WHEN r.accept_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.accept_time,'DD/MM/YYYY')::timestamp
        WHEN r.accept_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.accept_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.accept_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.accept_time,'DD-MM-YYYY HH24:MI')
        WHEN r.accept_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.accept_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS accept_time,

      -- depart_time
      CASE
        WHEN NULLIF(btrim(r.depart_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.depart_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.depart_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.depart_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.depart_time::date::timestamp
        WHEN r.depart_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.depart_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.depart_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.depart_time,'DD/MM/YYYY HH24:MI')
        WHEN r.depart_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.depart_time,'DD/MM/YYYY')::timestamp
        WHEN r.depart_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.depart_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.depart_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.depart_time,'DD-MM-YYYY HH24:MI')
        WHEN r.depart_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.depart_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS depart_time,

      -- arrive_time
      CASE
        WHEN NULLIF(btrim(r.arrive_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.arrive_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.arrive_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.arrive_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.arrive_time::date::timestamp
        WHEN r.arrive_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.arrive_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.arrive_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.arrive_time,'DD/MM/YYYY HH24:MI')
        WHEN r.arrive_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.arrive_time,'DD/MM/YYYY')::timestamp
        WHEN r.arrive_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.arrive_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.arrive_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.arrive_time,'DD-MM-YYYY HH24:MI')
        WHEN r.arrive_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.arrive_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS arrive_time,

      -- complete_time
      CASE
        WHEN NULLIF(btrim(r.complete_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.complete_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.complete_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.complete_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.complete_time::date::timestamp
        WHEN r.complete_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.complete_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.complete_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.complete_time,'DD/MM/YYYY HH24:MI')
        WHEN r.complete_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.complete_time,'DD/MM/YYYY')::timestamp
        WHEN r.complete_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.complete_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.complete_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.complete_time,'DD-MM-YYYY HH24:MI')
        WHEN r.complete_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.complete_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS complete_time,

      /* TEXT plano en ODS */
      left(NULLIF(btrim(r.require_finish_time),''), 500) AS require_finish_time,

      -- first_complete_time
      CASE
        WHEN NULLIF(btrim(r.first_complete_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.first_complete_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.first_complete_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.first_complete_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.first_complete_time::date::timestamp
        WHEN r.first_complete_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_complete_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.first_complete_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_complete_time,'DD/MM/YYYY HH24:MI')
        WHEN r.first_complete_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.first_complete_time,'DD/MM/YYYY')::timestamp
        WHEN r.first_complete_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_complete_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.first_complete_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_complete_time,'DD-MM-YYYY HH24:MI')
        WHEN r.first_complete_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.first_complete_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS first_complete_time,

      -- cancel_time
      CASE
        WHEN NULLIF(btrim(r.cancel_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.cancel_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.cancel_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.cancel_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.cancel_time::date::timestamp
        WHEN r.cancel_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.cancel_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.cancel_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.cancel_time,'DD/MM/YYYY HH24:MI')
        WHEN r.cancel_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.cancel_time,'DD/MM/YYYY')::timestamp
        WHEN r.cancel_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.cancel_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.cancel_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.cancel_time,'DD-MM-YYYY HH24:MI')
        WHEN r.cancel_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.cancel_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS cancel_time,

      NULLIF(btrim(r.cancel_operator),'')            AS cancel_operator,
      NULLIF(btrim(r.cancel_reason),'')              AS cancel_reason,
      NULLIF(btrim(r.task_status),'')                AS task_status,
      NULLIF(btrim(r.com_fault_speciality),'')       AS com_fault_speciality,
      NULLIF(btrim(r.com_fault_sub_speciality),'')   AS com_fault_sub_speciality,
      NULLIF(btrim(r.com_fault_cause),'')            AS com_fault_cause,
      NULLIF(btrim(r.leave_observations),'')         AS leave_observations,
      NULLIF(btrim(r.site_id),'')                    AS site_id,
      NULLIF(btrim(r.site_priority),'')              AS site_priority,
      NULLIF(btrim(r.title),'')                      AS title,
      NULLIF(btrim(r.description),'')                AS description,
      NULLIF(btrim(r.nro_toa),'')                    AS nro_toa,
      NULLIF(btrim(r.company_supply),'')             AS company_supply,
      NULLIF(btrim(r.zona),'')                       AS zona,
      NULLIF(btrim(r.departamento),'')               AS departamento,
      NULLIF(btrim(r.torrero),'')                    AS torrero,
      NULLIF(btrim(r.task_type),'')                  AS task_type,
      NULLIF(btrim(r.fm_office),'')                  AS fm_office,
      NULLIF(btrim(r.region),'')                     AS region,
      NULLIF(btrim(r.site_id_name),'')               AS site_id_name,
      NULLIF(btrim(r.site_location_type),'')         AS site_location_type,

      CASE WHEN NULLIF(btrim(r.sla),'') ~ '^-?\d+$'
           THEN r.sla::bigint ELSE NULL END          AS sla,
      NULLIF(btrim(r.sla_status),'')                 AS sla_status,
      NULLIF(btrim(r.suspend_state),'')              AS suspend_state,
      NULLIF(btrim(r.create_operator),'')            AS create_operator,

      -- fault_first_occur_time
      CASE
        WHEN NULLIF(btrim(r.fault_first_occur_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.fault_first_occur_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.fault_first_occur_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.fault_first_occur_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.fault_first_occur_time::date::timestamp
        WHEN r.fault_first_occur_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fault_first_occur_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.fault_first_occur_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fault_first_occur_time,'DD/MM/YYYY HH24:MI')
        WHEN r.fault_first_occur_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.fault_first_occur_time,'DD/MM/YYYY')::timestamp
        WHEN r.fault_first_occur_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fault_first_occur_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.fault_first_occur_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fault_first_occur_time,'DD-MM-YYYY HH24:MI')
        WHEN r.fault_first_occur_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.fault_first_occur_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS fault_first_occur_time,

      NULLIF(btrim(r.schedule_operator),'')          AS schedule_operator,

      -- schedule_time
      CASE
        WHEN NULLIF(btrim(r.schedule_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.schedule_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.schedule_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.schedule_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.schedule_time::date::timestamp
        WHEN r.schedule_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.schedule_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.schedule_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.schedule_time,'DD/MM/YYYY HH24:MI')
        WHEN r.schedule_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.schedule_time,'DD/MM/YYYY')::timestamp
        WHEN r.schedule_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.schedule_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.schedule_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.schedule_time,'DD-MM-YYYY HH24:MI')
        WHEN r.schedule_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.schedule_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS schedule_time,

      NULLIF(btrim(r.assign_to_fme),'')              AS assign_to_fme,
      NULLIF(btrim(r.assign_to_fme_full_name),'')    AS assign_to_fme_full_name,
      NULLIF(btrim(r.dispatch_operator),'')          AS dispatch_operator,
      NULLIF(btrim(r.depart_operator),'')            AS depart_operator,
      NULLIF(btrim(r.arrive_operator),'')            AS arrive_operator,
      NULLIF(btrim(r.complete_operator),'')          AS complete_operator,

      /* TEXT en ODS */
      NULLIF(btrim(r.reject_time),'')                AS reject_time,
      NULLIF(btrim(r.reject_operator),'')            AS reject_operator,
      NULLIF(btrim(r.reject_des),'')                 AS reject_des,
      NULLIF(btrim(r.com_level_1_aff_equip),'')      AS com_level_1_aff_equip,
      NULLIF(btrim(r.com_level_2_aff_equip),'')      AS com_level_2_aff_equip,
      NULLIF(btrim(r.com_level_3_aff_equip),'')      AS com_level_3_aff_equip,
      NULLIF(btrim(r.fault_type),'')                 AS fault_type,
      NULLIF(btrim(r.task_category),'')              AS task_category,
      NULLIF(btrim(r.task_subcategory),'')           AS task_subcategory,
      NULLIF(btrim(r.fme_contrator),'')              AS fme_contrator,
      NULLIF(btrim(r.contratista_sitio),'')          AS contratista_sitio,
      NULLIF(btrim(r.complete_operator_name),'')     AS complete_operator_name,
      NULLIF(btrim(r.reject_operator_name),'')       AS reject_operator_name,
      NULLIF(btrim(r.arrive_operator_name),'')       AS arrive_operator_name,
      NULLIF(btrim(r.depart_operator_name),'')       AS depart_operator_name,
      NULLIF(btrim(r.dispatch_operator_name),'')     AS dispatch_operator_name,
      NULLIF(btrim(r.cancel_operator_name),'')       AS cancel_operator_name,
      NULLIF(btrim(r.schedule_operator_name),'')     AS schedule_operator_name,
      NULLIF(btrim(r.create_operator_name),'')       AS create_operator_name,
      NULLIF(btrim(r.situacion_encontrada),'')       AS situacion_encontrada,
      NULLIF(btrim(r.detalle_de_actuacion_realizada),'') AS detalle_de_actuacion_realizada,
      NULLIF(btrim(r.atencion_incidencias),'')       AS atencion_incidencias,
      NULLIF(btrim(r.remedy_id),'')                  AS remedy_id,

      -- first_pause_time
      CASE
        WHEN NULLIF(btrim(r.first_pause_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.first_pause_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.first_pause_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.first_pause_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.first_pause_time::date::timestamp
        WHEN r.first_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_pause_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.first_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_pause_time,'DD/MM/YYYY HH24:MI')
        WHEN r.first_pause_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.first_pause_time,'DD/MM/YYYY')::timestamp
        WHEN r.first_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_pause_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.first_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_pause_time,'DD-MM-YYYY HH24:MI')
        WHEN r.first_pause_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.first_pause_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS first_pause_time,

      NULLIF(btrim(r.first_pause_operator),'')       AS first_pause_operator,
      NULLIF(btrim(r.first_pause_reason),'')         AS first_pause_reason,
      NULLIF(btrim(r.first_resume_operator),'')      AS first_resume_operator,

      -- first_resume_time
      CASE
        WHEN NULLIF(btrim(r.first_resume_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.first_resume_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.first_resume_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.first_resume_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.first_resume_time::date::timestamp
        WHEN r.first_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_resume_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.first_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_resume_time,'DD/MM/YYYY HH24:MI')
        WHEN r.first_resume_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.first_resume_time,'DD/MM/YYYY')::timestamp
        WHEN r.first_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_resume_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.first_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_resume_time,'DD-MM-YYYY HH24:MI')
        WHEN r.first_resume_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.first_resume_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS first_resume_time,

      -- second_pause_time
      CASE
        WHEN NULLIF(btrim(r.second_pause_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.second_pause_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.second_pause_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.second_pause_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.second_pause_time::date::timestamp
        WHEN r.second_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.second_pause_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.second_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.second_pause_time,'DD/MM/YYYY HH24:MI')
        WHEN r.second_pause_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.second_pause_time,'DD/MM/YYYY')::timestamp
        WHEN r.second_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.second_pause_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.second_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.second_pause_time,'DD-MM-YYYY HH24:MI')
        WHEN r.second_pause_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.second_pause_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS second_pause_time,

      NULLIF(btrim(r.second_pause_reason),'')        AS second_pause_reason,
      NULLIF(btrim(r.second_pause_operator),'')      AS second_pause_operator,

      -- second_resume_time
      CASE
        WHEN NULLIF(btrim(r.second_resume_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.second_resume_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.second_resume_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.second_resume_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.second_resume_time::date::timestamp
        WHEN r.second_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.second_resume_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.second_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.second_resume_time,'DD/MM/YYYY HH24:MI')
        WHEN r.second_resume_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.second_resume_time,'DD/MM/YYYY')::timestamp
        WHEN r.second_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.second_resume_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.second_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.second_resume_time,'DD-MM-YYYY HH24:MI')
        WHEN r.second_resume_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.second_resume_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS second_resume_time,

      NULLIF(btrim(r.second_resume_operator),'')     AS second_resume_operator,

      -- third_pause_time
      CASE
        WHEN NULLIF(btrim(r.third_pause_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.third_pause_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.third_pause_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.third_pause_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.third_pause_time::date::timestamp
        WHEN r.third_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.third_pause_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.third_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.third_pause_time,'DD/MM/YYYY HH24:MI')
        WHEN r.third_pause_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.third_pause_time,'DD/MM/YYYY')::timestamp
        WHEN r.third_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.third_pause_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.third_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.third_pause_time,'DD-MM-YYYY HH24:MI')
        WHEN r.third_pause_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.third_pause_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS third_pause_time,

      NULLIF(btrim(r.third_pause_reason),'')         AS third_pause_reason,
      NULLIF(btrim(r.third_pause_operator),'')       AS third_pause_operator,

      -- third_resume_time
      CASE
        WHEN NULLIF(btrim(r.third_resume_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.third_resume_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.third_resume_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.third_resume_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.third_resume_time::date::timestamp
        WHEN r.third_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.third_resume_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.third_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.third_resume_time,'DD/MM/YYYY HH24:MI')
        WHEN r.third_resume_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.third_resume_time,'DD/MM/YYYY')::timestamp
        WHEN r.third_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.third_resume_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.third_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.third_resume_time,'DD-MM-YYYY HH24:MI')
        WHEN r.third_resume_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.third_resume_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS third_resume_time,

      NULLIF(btrim(r.third_resume_operator),'')      AS third_resume_operator,

      -- fourth_pause_time
      CASE
        WHEN NULLIF(btrim(r.fourth_pause_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.fourth_pause_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.fourth_pause_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.fourth_pause_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.fourth_pause_time::date::timestamp
        WHEN r.fourth_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_pause_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.fourth_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_pause_time,'DD/MM/YYYY HH24:MI')
        WHEN r.fourth_pause_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.fourth_pause_time,'DD/MM/YYYY')::timestamp
        WHEN r.fourth_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_pause_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.fourth_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_pause_time,'DD-MM-YYYY HH24:MI')
        WHEN r.fourth_pause_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.fourth_pause_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS fourth_pause_time,

      NULLIF(btrim(r.fourth_pause_reason),'')        AS fourth_pause_reason,
      NULLIF(btrim(r.fourth_pause_operator),'')      AS fourth_pause_operator,

      -- fourth_resume_time
      CASE
        WHEN NULLIF(btrim(r.fourth_resume_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.fourth_resume_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.fourth_resume_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.fourth_resume_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.fourth_resume_time::date::timestamp
        WHEN r.fourth_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_resume_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.fourth_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_resume_time,'DD/MM/YYYY HH24:MI')
        WHEN r.fourth_resume_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.fourth_resume_time,'DD/MM/YYYY')::timestamp
        WHEN r.fourth_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_resume_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.fourth_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_resume_time,'DD-MM-YYYY HH24:MI')
        WHEN r.fourth_resume_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.fourth_resume_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS fourth_resume_time,

      NULLIF(btrim(r.fourth_resume_operator),'')     AS fourth_resume_operator,

      -- fifth_pause_time
      CASE
        WHEN NULLIF(btrim(r.fifth_pause_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.fifth_pause_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.fifth_pause_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.fifth_pause_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.fifth_pause_time::date::timestamp
        WHEN r.fifth_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_pause_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.fifth_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_pause_time,'DD/MM/YYYY HH24:MI')
        WHEN r.fifth_pause_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.fifth_pause_time,'DD/MM/YYYY')::timestamp
        WHEN r.fifth_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_pause_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.fifth_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_pause_time,'DD-MM-YYYY HH24:MI')
        WHEN r.fifth_pause_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.fifth_pause_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS fifth_pause_time,

      NULLIF(btrim(r.fifth_pause_reason),'')         AS fifth_pause_reason,
      NULLIF(btrim(r.fifth_pause_operator),'')       AS fifth_pause_operator,

      -- fifth_resume_time
      CASE
        WHEN NULLIF(btrim(r.fifth_resume_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.fifth_resume_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.fifth_resume_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.fifth_resume_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.fifth_resume_time::date::timestamp
        WHEN r.fifth_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_resume_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.fifth_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_resume_time,'DD/MM/YYYY HH24:MI')
        WHEN r.fifth_resume_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.fifth_resume_time,'DD/MM/YYYY')::timestamp
        WHEN r.fifth_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_resume_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.fifth_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_resume_time,'DD-MM-YYYY HH24:MI')
        WHEN r.fifth_resume_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.fifth_resume_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS fifth_resume_time,

      NULLIF(btrim(r.fifth_resume_operator),'')      AS fifth_resume_operator,

      NULLIF(btrim(r.reject_flag),'')                AS reject_flag,
      CASE WHEN NULLIF(btrim(r.reject_counter),'') ~ '^-?\d+$'
           THEN r.reject_counter::bigint ELSE NULL END AS reject_counter,
      NULLIF(btrim(r.fuel_type),'')                  AS fuel_type
    FROM raw.web_mm_autin_infogeneral r
    WHERE NULLIF(btrim(r.task_id),'') IS NOT NULL
  ),
  dedup AS (
    SELECT DISTINCT ON (task_id) *
    FROM stg
    ORDER BY task_id
  ),
  upd AS (
    UPDATE ods.web_hm_autin_infogeneral t
    SET
      createtime                    = s.createtime,
      dispatch_time                 = s.dispatch_time,
      accept_time                   = s.accept_time,
      depart_time                   = s.depart_time,
      arrive_time                   = s.arrive_time,
      complete_time                 = s.complete_time,
      require_finish_time           = s.require_finish_time,
      first_complete_time           = s.first_complete_time,
      cancel_time                   = s.cancel_time,
      cancel_operator               = s.cancel_operator,
      cancel_reason                 = s.cancel_reason,
      task_status                   = s.task_status,
      com_fault_speciality          = s.com_fault_speciality,
      com_fault_sub_speciality      = s.com_fault_sub_speciality,
      com_fault_cause               = s.com_fault_cause,
      leave_observations            = s.leave_observations,
      site_id                       = s.site_id,
      site_priority                 = s.site_priority,
      title                         = s.title,
      description                   = s.description,
      nro_toa                       = s.nro_toa,
      company_supply                = s.company_supply,
      zona                          = s.zona,
      departamento                  = s.departamento,
      torrero                       = s.torrero,
      task_type                     = s.task_type,
      fm_office                     = s.fm_office,
      region                        = s.region,
      site_id_name                  = s.site_id_name,
      site_location_type            = s.site_location_type,
      sla                           = s.sla,
      sla_status                    = s.sla_status,
      suspend_state                 = s.suspend_state,
      create_operator               = s.create_operator,
      fault_first_occur_time        = s.fault_first_occur_time,
      schedule_operator             = s.schedule_operator,
      schedule_time                 = s.schedule_time,
      assign_to_fme                 = s.assign_to_fme,
      assign_to_fme_full_name       = s.assign_to_fme_full_name,
      dispatch_operator             = s.dispatch_operator,
      depart_operator               = s.depart_operator,
      arrive_operator               = s.arrive_operator,
      complete_operator             = s.complete_operator,
      reject_time                   = s.reject_time,
      reject_operator               = s.reject_operator,
      reject_des                    = s.reject_des,
      com_level_1_aff_equip         = s.com_level_1_aff_equip,
      com_level_2_aff_equip         = s.com_level_2_aff_equip,
      com_level_3_aff_equip         = s.com_level_3_aff_equip,
      fault_type                    = s.fault_type,
      task_category                 = s.task_category,
      task_subcategory              = s.task_subcategory,
      fme_contrator                 = s.fme_contrator,
      contratista_sitio             = s.contratista_sitio,
      complete_operator_name        = s.complete_operator_name,
      reject_operator_name          = s.reject_operator_name,
      arrive_operator_name          = s.arrive_operator_name,
      depart_operator_name          = s.depart_operator_name,
      dispatch_operator_name        = s.dispatch_operator_name,
      cancel_operator_name          = s.cancel_operator_name,
      schedule_operator_name        = s.schedule_operator_name,
      create_operator_name          = s.create_operator_name,
      situacion_encontrada          = s.situacion_encontrada,
      detalle_de_actuacion_realizada= s.detalle_de_actuacion_realizada,
      atencion_incidencias          = s.atencion_incidencias,
      remedy_id                     = s.remedy_id,
      first_pause_time              = s.first_pause_time,
      first_pause_operator          = s.first_pause_operator,
      first_pause_reason            = s.first_pause_reason,
      first_resume_operator         = s.first_resume_operator,
      first_resume_time             = s.first_resume_time,
      second_pause_time             = s.second_pause_time,
      second_pause_reason           = s.second_pause_reason,
      second_pause_operator         = s.second_pause_operator,
      second_resume_time            = s.second_resume_time,
      second_resume_operator        = s.second_resume_operator,
      third_pause_time              = s.third_pause_time,
      third_pause_reason            = s.third_pause_reason,
      third_pause_operator          = s.third_pause_operator,
      third_resume_time             = s.third_resume_time,
      third_resume_operator         = s.third_resume_operator,
      fourth_pause_time             = s.fourth_pause_time,
      fourth_pause_reason           = s.fourth_pause_reason,
      fourth_pause_operator         = s.fourth_pause_operator,
      fourth_resume_time            = s.fourth_resume_time,
      fourth_resume_operator        = s.fourth_resume_operator,
      fifth_pause_time              = s.fifth_pause_time,
      fifth_pause_reason            = s.fifth_pause_reason,
      fifth_pause_operator          = s.fifth_pause_operator,
      fifth_resume_time             = s.fifth_resume_time,
      fifth_resume_operator         = s.fifth_resume_operator,
      reject_flag                   = s.reject_flag,
      reject_counter                = s.reject_counter,
      fuel_type                     = s.fuel_type
    FROM dedup s
    WHERE t.task_id = s.task_id
    RETURNING 1
  ),
  ins AS (
    INSERT INTO ods.web_hm_autin_infogeneral (
      task_id, createtime, dispatch_time, accept_time, depart_time, arrive_time, complete_time,
      require_finish_time, first_complete_time, cancel_time, cancel_operator, cancel_reason, task_status,
      com_fault_speciality, com_fault_sub_speciality, com_fault_cause, leave_observations, site_id,
      site_priority, title, description, nro_toa, company_supply, zona, departamento, torrero, task_type,
      fm_office, region, site_id_name, site_location_type, sla, sla_status, suspend_state, create_operator,
      fault_first_occur_time, schedule_operator, schedule_time, assign_to_fme, assign_to_fme_full_name,
      dispatch_operator, depart_operator, arrive_operator, complete_operator, reject_time, reject_operator,
      reject_des, com_level_1_aff_equip, com_level_2_aff_equip, com_level_3_aff_equip, fault_type,
      task_category, task_subcategory, fme_contrator, contratista_sitio, complete_operator_name,
      reject_operator_name, arrive_operator_name, depart_operator_name, dispatch_operator_name,
      cancel_operator_name, schedule_operator_name, create_operator_name, situacion_encontrada,
      detalle_de_actuacion_realizada, atencion_incidencias, remedy_id, first_pause_time, first_pause_operator,
      first_pause_reason, first_resume_operator, first_resume_time, second_pause_time, second_pause_reason,
      second_pause_operator, second_resume_time, second_resume_operator, third_pause_time, third_pause_reason,
      third_pause_operator, third_resume_time, third_resume_operator, fourth_pause_time, fourth_pause_reason,
      fourth_pause_operator, fourth_resume_time, fourth_resume_operator, fifth_pause_time, fifth_pause_reason,
      fifth_pause_operator, fifth_resume_time, fifth_resume_operator, reject_flag, reject_counter, fuel_type
    )
    SELECT
      s.task_id, s.createtime, s.dispatch_time, s.accept_time, s.depart_time, s.arrive_time, s.complete_time,
      s.require_finish_time, s.first_complete_time, s.cancel_time, s.cancel_operator, s.cancel_reason, s.task_status,
      s.com_fault_speciality, s.com_fault_sub_speciality, s.com_fault_cause, s.leave_observations, s.site_id,
      s.site_priority, s.title, s.description, s.nro_toa, s.company_supply, s.zona, s.departamento, s.torrero, s.task_type,
      s.fm_office, s.region, s.site_id_name, s.site_location_type, s.sla, s.sla_status, s.suspend_state, s.create_operator,
      s.fault_first_occur_time, s.schedule_operator, s.schedule_time, s.assign_to_fme, s.assign_to_fme_full_name,
      s.dispatch_operator, s.depart_operator, s.arrive_operator, s.complete_operator, s.reject_time, s.reject_operator,
      s.reject_des, s.com_level_1_aff_equip, s.com_level_2_aff_equip, s.com_level_3_aff_equip, s.fault_type,
      s.task_category, s.task_subcategory, s.fme_contrator, s.contratista_sitio, s.complete_operator_name,
      s.reject_operator_name, s.arrive_operator_name, s.depart_operator_name, s.dispatch_operator_name,
      s.cancel_operator_name, s.schedule_operator_name, s.create_operator_name, s.situacion_encontrada,
      s.detalle_de_actuacion_realizada, s.atencion_incidencias, s.remedy_id, s.first_pause_time, s.first_pause_operator,
      s.first_pause_reason, s.first_resume_operator, s.first_resume_time, s.second_pause_time, s.second_pause_reason,
      s.second_pause_operator, s.second_resume_time, s.second_resume_operator, s.third_pause_time, s.third_pause_reason,
      s.third_pause_operator, s.third_resume_time, s.third_resume_operator, s.fourth_pause_time, s.fourth_pause_reason,
      s.fourth_pause_operator, s.fourth_resume_time, s.fourth_resume_operator, s.fifth_pause_time, s.fifth_pause_reason,
      s.fifth_pause_operator, s.fifth_resume_time, s.fifth_resume_operator, s.reject_flag, s.reject_counter, s.fuel_type
    FROM dedup s
    LEFT JOIN ods.web_hm_autin_infogeneral t
      ON t.task_id = s.task_id
    WHERE t.task_id IS NULL
    RETURNING 1
  )
  SELECT
    COALESCE((SELECT COUNT(*) FROM upd), 0) AS updated,
    COALESCE((SELECT COUNT(*) FROM ins), 0) AS inserted
  INTO v_updated, v_inserted;

  v_estado := 'DONE';
  v_msj := format('Insertados en ODS: %s | Upd: %s | Del: %s | Origen: raw.web_mm_autin_infogeneral',
                  v_inserted, v_updated, v_deleted);

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
    v_estado := 'ERROR';
    v_msj := SQLERRM;
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
    RAISE;
END;
$procedure$
;


