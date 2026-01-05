CREATE TABLE ods.web_hd_neteco_diaria (
	site_name text NOT NULL,
	manage_object text NOT NULL,
	subnet text NULL,
	fecha date NOT NULL,
	energy_consumption_per_day_kwh numeric(18, 4) NULL,
	supply_duration_per_day_h numeric(18, 4) NULL,
	total_energy_consumption_per_day_kwh numeric(18, 4) NULL,
	creation_user varchar(100) DEFAULT (((COALESCE(inet_client_addr()::text, 'local'::text) || '-'::text) || CURRENT_USER::text)) NULL,
	creation_date timestamp(0) DEFAULT clock_timestamp()::timestamp(0) without time zone NULL,
	creation_ip inet DEFAULT inet_client_addr() NULL,
	CONSTRAINT pk_web_hd_neteco_diaria PRIMARY KEY (site_name, manage_object, fecha)
);