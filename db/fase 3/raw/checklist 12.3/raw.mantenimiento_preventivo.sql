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
