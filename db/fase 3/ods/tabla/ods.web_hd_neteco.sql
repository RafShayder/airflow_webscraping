CREATE TABLE ods.web_hd_neteco (
	site_name text NOT NULL,
	manage_object text NOT NULL,
	start_time timestamp NOT NULL,
	subnet text NULL,
	energy_consumption_per_hour numeric(18, 4) NULL,
	supply_duration_per_hour numeric(18, 4) NULL,
	total_energy_consumption numeric(18, 4) NULL,
	archivo text NULL,
	fecha_ultima_actualizacion timestamp DEFAULT clock_timestamp() NOT NULL,
	CONSTRAINT pk_web_md_neteco PRIMARY KEY (site_name, manage_object, start_time)
);