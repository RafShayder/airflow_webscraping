CREATE TABLE raw.mantenimiento_preventivo_dinami (
  task_id      varchar(255) NOT NULL,
  site_id      varchar(255) NOT NULL,
  sub_wo_id    varchar(255) NOT NULL,
  work_order   text NULL,
  fechacarga   timestamp(0) NOT NULL
);

CREATE INDEX raw_mantenimiento_preventivo_dinami_fechacarga_idx
ON raw.mantenimiento_preventivo_dinami USING btree (fechacarga);
