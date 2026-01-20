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
    'raw.ie_tablero_secundario',

    -- SOLARES
    'raw.sol_banco_de_baterias_solares',
    'raw.sol_controlador_solar',
    'raw.sol_informacion_general_sist',
    'raw.sol_paneles_solares',

    -- TRANSMISIÓN / TX
    'raw.tx_bh_2_4_6_12_gwc',
    'raw.tx_bh_2_4_6_12_gwd',
    'raw.tx_bh_2_4_6_12_gwt',
    'raw.tx_2_4_6_12_antena',
    'raw.tx_2_4_6_12_dwdm',
    'raw.tx_2_4_6_12_idu',
    'raw.tx_2_4_6_12_odu',
    'raw.tx_2_4_6_12_sdh',

    -- GE
    'raw.ge_grupo_electrogeno',
    'raw.ge_info_general_de_tanque',
    'raw.ge_limp_interna_tk',
    'raw.ge_tablero_de_transferencia_a',
    'raw.ge_transferencia_con_carga',

    -- RADIO
    'raw.radio_6_12_18_bas_cf_bb',
    'raw.radio_6_12_18_bbu',

    -- SE
    'raw.se_banco_de_condensadores',
    'raw.se_proteccion_y_pararrayos',
    'raw.se_tablero_de_paso_de_salida',
    'raw.se_trafomix',
    'raw.se_transformador_de_potencia',

    -- UPS / INVERSOR / AVR
    'raw.ups_ups',
    'raw.ups_bateria_de_ups',
    'raw.inversor',
    'raw.avr',

    -- CLIMA
    'raw.clima',
    'raw.clima_condensador',
    'raw.clima_evaporador',

    -- LÍNEA MT/BT
    'raw.lmt_lbt_conductores_y_protecc',
    'raw.lmt_lbt_datos_de_linea_genera',
    'raw.lmt_lbt_poste_de_cada_uno',

    -- MANTENIMIENTO
    'raw.mantenimiento_preventivo',
    'raw.mantenimiento_preventivo_dinami'
  ];

  v_t        text;
  v_keep_ts  timestamp;
  v_borradas bigint;
  v_detalle  text := '';
BEGIN
  FOREACH v_t IN ARRAY v_tablas LOOP
    BEGIN
      -- 1) Asegurar índice por fechacarga
      EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %s (fechacarga)',
        replace(v_t, '.', '_') || '_fechacarga_idx', v_t
      );

      -- 2) Obtener el máximo fechacarga del día p_fecha
      EXECUTE format(
        'SELECT MAX(fechacarga) FROM %s WHERE fechacarga::date = $1', v_t
      ) USING p_fecha INTO v_keep_ts;

      IF v_keep_ts IS NULL THEN
        v_detalle := v_detalle || format('%s: sin filas %s; ', v_t, p_fecha);
        CONTINUE;
      END IF;

      -- 3) Borrar duplicados del mismo día, conservando solo el último lote
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
$procedure$;
