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
