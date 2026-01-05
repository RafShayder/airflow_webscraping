
CREATE OR REPLACE PROCEDURE public.sp_grabar_log_sp(IN p_id_sp integer DEFAULT NULL::integer, IN p_inicio timestamp without time zone DEFAULT NULL::timestamp without time zone, IN p_fin timestamp without time zone DEFAULT NULL::timestamp without time zone, IN p_inserted integer DEFAULT NULL::integer, IN p_updated integer DEFAULT NULL::integer, IN p_deleted integer DEFAULT NULL::integer, IN p_nulls integer DEFAULT NULL::integer, IN p_estado character varying DEFAULT NULL::character varying, IN p_msj_error text DEFAULT NULL::text, IN p_sp text DEFAULT NULL::text)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
  IF p_inicio IS NULL THEN p_inicio := clock_timestamp()::timestamp(0); END IF;
  IF p_fin    IS NULL THEN p_fin    := clock_timestamp()::timestamp(0); END IF;

  INSERT INTO public.log_sp (
    id_sp, sp, inicio, fin,
    inserted, updated, deleted, nulls, estado, msj_error
  ) VALUES (
    p_id_sp, p_sp, p_inicio, p_fin,
    p_inserted, p_updated, p_deleted, p_nulls, p_estado, p_msj_error
  );
END;
$procedure$
;

