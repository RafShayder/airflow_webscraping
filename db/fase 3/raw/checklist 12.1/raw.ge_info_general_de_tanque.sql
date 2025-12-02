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
