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
