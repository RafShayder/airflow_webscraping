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
