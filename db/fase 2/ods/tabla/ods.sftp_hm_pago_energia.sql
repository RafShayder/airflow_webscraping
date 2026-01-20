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