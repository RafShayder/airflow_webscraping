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