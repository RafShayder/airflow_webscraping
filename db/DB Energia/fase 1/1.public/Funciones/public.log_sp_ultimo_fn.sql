-- DROP FUNCTION public.log_sp_ultimo_fn(text);

CREATE OR REPLACE FUNCTION public.log_sp_ultimo_fn(p_sp text)
 RETURNS TABLE(id integer, id_sp integer, sp text, inicio timestamp without time zone, fin timestamp without time zone, inserted integer, updated integer, deleted integer, nulls integer, estado character varying, msj_error text, usr_cre text)
 LANGUAGE sql
AS $function$
  SELECT
    id, id_sp, sp, inicio, fin, inserted, updated, deleted, "nulls",
    estado, msj_error, usr_cre
  FROM public.log_sp
  WHERE sp = p_sp
  ORDER BY COALESCE(fin, inicio, 'epoch'::timestamp) DESC, id DESC
  LIMIT 1
$function$
;
