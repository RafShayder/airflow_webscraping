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
