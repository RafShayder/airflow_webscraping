CREATE TABLE raw.lmt_lbt_poste_de_cada_uno (
  task_id      varchar(255) NOT NULL,
  site_id      varchar(255) NOT NULL,
  sub_wo_id    varchar(255) NOT NULL,
  fechacarga   timestamp(0) NOT NULL
);

CREATE INDEX raw_lmt_lbt_poste_de_cada_uno_fechacarga_idx
ON raw.lmt_lbt_poste_de_cada_uno USING btree (fechacarga);
