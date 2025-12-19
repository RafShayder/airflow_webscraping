CREATE TABLE raw.web_md_neteco (
	site_name text NULL,
	subnet text NULL,
	manage_object text NULL,
	start_time text NULL,
	energy_consumption_per_hour_kwh text NULL,
	supply_duration_per_hour_h text NULL,
	total_energy_consumption_kwh text NULL,
	archivo text NULL,
	fecha_carga timestamptz DEFAULT clock_timestamp() NOT NULL
);
CREATE INDEX idx_web_md_neteco_fecha_carga ON raw.web_md_neteco USING btree (fecha_carga);