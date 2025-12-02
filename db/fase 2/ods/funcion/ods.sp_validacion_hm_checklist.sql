-- DROP PROCEDURE ods.sp_validacion_hm_checklist(date);

CREATE OR REPLACE PROCEDURE ods.sp_validacion_hm_checklist(IN p_fecha date DEFAULT CURRENT_DATE)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio    timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp     integer      := 'ods.sp_validacion_hm_checklist(date)'::regprocedure::oid::int;
  v_sp_name   text         := 'ods.sp_validacion_hm_checklist(date)'::regprocedure::text;

  v_inserted  integer := 0;
  v_updated   integer := 0;
  v_deleted   integer := 0;
  v_nulls     integer := NULL;
  v_estado    varchar(50);
  v_msj       text;

  v_tablas CONSTANT text[] := ARRAY[
    -- CF / IE (ya existentes)
    'raw.cf_banco_de_baterias',
    'raw.cf_bastidor_distribucion',
    'raw.cf_cuadro_de_fuerza',
    'raw.cf_modulos_rectificadores',
    'raw.cf_tablero_ac_de_cuadro_de_fu',
    'raw.cf_descarga_controlada_bater',
    'raw.ie_datos_spat_general',
    'raw.ie_mantenimiento_pozo_por_poz',
    'raw.ie_suministro_de_energia',
    'raw.ie_tablero_principal',
    'raw.ie_tablero_secundario'

    -- GE / RADIO / SE (nuevas)
    --'raw.ge_grupo_electrogeno',
    --'raw.ge_info_general_de_tanque',
    --'raw.ge_limp_interna_tk',
    --'raw.ge_tablero_de_transferencia_a',
    --'raw.ge_transferencia_con_carga',
    --'raw.radio_6_12_18_bas_cf_bb',
    --'raw.radio_6_12_18_bbu',
    --'raw.se_banco_de_condensadores',
    --'raw.se_proteccion_y_pararrayos',
    --'raw.se_tablero_de_paso_de_salida',
    --'raw.se_trafomix',
    --'raw.se_transformador_de_potencia'
  ];

  v_t        text;
  v_keep_ts  timestamp;
  v_borradas bigint;
  v_detalle  text := '';
BEGIN
  FOREACH v_t IN ARRAY v_tablas LOOP
    BEGIN
      EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %s (fechacarga)',
        replace(v_t, '.', '_') || '_fechacarga_idx', v_t
      );

      EXECUTE format(
        'SELECT MAX(fechacarga) FROM %s WHERE fechacarga::date = $1', v_t
      ) USING p_fecha INTO v_keep_ts;

      IF v_keep_ts IS NULL THEN
        v_detalle := v_detalle || format('%s: sin filas %s; ', v_t, p_fecha);
        CONTINUE;
      END IF;

      EXECUTE format(
        'DELETE FROM %s WHERE fechacarga::date = $1 AND fechacarga <> $2', v_t
      ) USING p_fecha, v_keep_ts;

      GET DIAGNOSTICS v_borradas = ROW_COUNT;
      v_deleted := v_deleted + COALESCE(v_borradas,0);

      v_detalle := v_detalle
        || format('%s: keep=%s; borradas=%s; ',
                  v_t, to_char(v_keep_ts,'YYYY-MM-DD HH24:MI:SS'), v_borradas);
    EXCEPTION
      WHEN undefined_column THEN
        v_detalle := v_detalle || format('%s: ERROR sin columna fechacarga; ', v_t);
      WHEN OTHERS THEN
        v_detalle := v_detalle || format('%s: ERROR %s; ', v_t, SQLERRM);
    END;
  END LOOP;

  v_estado := 'DONE';
  v_msj := format('Validación HM Checklist (fecha=%s). Borradas=%s. %s',
                  p_fecha, v_deleted, v_detalle);

  CALL public.sp_grabar_log_sp(
    p_id_sp      => v_id_sp,
    p_inicio     => v_inicio,
    p_fin        => clock_timestamp()::timestamp(0),
    p_inserted   => v_inserted,
    p_updated    => v_updated,
    p_deleted    => v_deleted,
    p_nulls      => v_nulls,
    p_estado     => v_estado,
    p_msj_error  => v_msj,
    p_sp         => v_sp_name
  );
EXCEPTION
  WHEN OTHERS THEN
    CALL public.sp_grabar_log_sp(
      p_id_sp      => v_id_sp,
      p_inicio     => v_inicio,
      p_fin        => clock_timestamp()::timestamp(0),
      p_inserted   => v_inserted,
      p_updated    => v_updated,
      p_deleted    => v_deleted,
      p_nulls      => v_nulls,
      p_estado     => 'ERROR',
      p_msj_error  => SQLERRM,
      p_sp         => v_sp_name
    );
    RAISE;
END;
$procedure$
;
