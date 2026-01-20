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