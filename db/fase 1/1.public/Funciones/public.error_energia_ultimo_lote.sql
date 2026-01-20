CREATE OR REPLACE FUNCTION public.error_energia_ultimo_lote(p_tabla text)
 RETURNS SETOF error_energia
 LANGUAGE sql
 STABLE
AS $function$
  SELECT e.*
  FROM public.error_energia e
  WHERE lower(btrim(e.tabla_origen)) = lower(btrim(p_tabla))
    AND e.fecha_carga::date = CURRENT_DATE
    AND e.fecha_carga = (
      SELECT MAX(fecha_carga)
      FROM public.error_energia
      WHERE lower(btrim(tabla_origen)) = lower(btrim(p_tabla))
        AND fecha_carga::date = CURRENT_DATE
    );
$function$
;

