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
