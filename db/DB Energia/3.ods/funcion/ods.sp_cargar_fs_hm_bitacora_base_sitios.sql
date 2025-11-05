
CREATE OR REPLACE PROCEDURE ods.sp_cargar_fs_hm_bitacora_base_sitios()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio    timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp     integer      := 'ods.sp_cargar_fs_hm_bitacora_base_sitios()'::regprocedure::oid::int;
  v_sp_name   text         := 'ods.sp_cargar_fs_hm_bitacora_base_sitios()'::regprocedure::text;

  v_inserted  integer := 0;
  v_updated   integer := 0;
  v_deleted   integer := 0;
  v_nulls     integer := NULL;

  v_estado    varchar(50);
  v_msj       text;
BEGIN
  /* 1) Contamos filas actuales y truncamos destino (full refresh) */
  SELECT COUNT(*) INTO v_deleted FROM ods.fs_hm_bitacora_base_sitios;
  TRUNCATE TABLE ods.fs_hm_bitacora_base_sitios;
  -- Opcional: TRUNCATE TABLE ods.fs_hm_bitacora_base_sitios RESTART IDENTITY;

  WITH
  raw_clean AS (
    SELECT
      NULLIF(TRIM(r.codigo_unico),'')                          AS cu,
      LEFT(NULLIF(TRIM(r.nombre_local),''), 200)               AS nombre_local,
      LEFT(NULLIF(TRIM(r.direccion),''), 350)                  AS direccion,

      /* ===== LATITUD ===== */
      (
        CASE
          -- Decimal
          WHEN TRIM(r.latitud) ~* '^\s*[+-]?\d+(?:[.,]\d+)?\s*[NSEWO]?\s*$' THEN
            ( NULLIF(REPLACE(TRIM(regexp_replace(r.latitud, '[^0-9\+\-.,]', '', 'g')), ',', '.'), '')::numeric )
            * CASE WHEN TRIM(r.latitud) ~* '[SWO]' THEN -1 ELSE 1 END

          -- DMS
          WHEN TRIM(r.latitud) ~* '^\s*\d+(?:\s*[°º]\s*\d+)?(?:\s*[''’′m]\s*\d+(?:[.,]\d+)?)?(?:\s*["”″s])?\s*[NSEWO]?\s*$' THEN
            (
              COALESCE((regexp_match(TRIM(r.latitud),
                '^\s*(\d+)(?:\s*[°º]\s*(\d+))?(?:\s*[''’′m]\s*(\d+(?:[.,]\d+)?))?(?:\s*["”″s])?\s*([NnSsEeWwOo])?\s*$'
              ))[1], '0')::numeric
              + COALESCE((regexp_match(TRIM(r.latitud),
                '^\s*(\d+)(?:\s*[°º]\s*(\d+))?(?:\s*[''’′m]\s*(\d+(?:[.,]\d+)?))?(?:\s*["”″s])?\s*([NnSsEeWwOo])?\s*$'
              ))[2], '0')::numeric / 60
              + COALESCE(REPLACE(COALESCE((regexp_match(TRIM(r.latitud),
                '^\s*(\d+)(?:\s*[°º]\s*(\d+))?(?:\s*[''’′m]\s*(\d+(?:[.,]\d+)?))?(?:\s*["”″s])?\s*([NnSsEeWwOo])?\s*$'
              ))[3], '0'), ',', '.')::numeric, 0) / 3600
            ) * CASE
                  WHEN COALESCE((regexp_match(TRIM(r.latitud),
                    '^\s*(\d+)(?:\s*[°º]\s*(\d+))?(?:\s*[''’′m]\s*(\d+(?:[.,]\d+)?))?(?:\s*["”″s])?\s*([NnSsEeWwOo])?\s*$'
                  ))[4], 'N') ~* '^[SWO]$' THEN -1 ELSE 1
                END
          ELSE NULL
        END
      )::numeric(11,8) AS latitud,

      /* ===== LONGITUD ===== */
      (
        CASE
          -- Decimal
          WHEN TRIM(r.longitud) ~* '^\s*[+-]?\d+(?:[.,]\d+)?\s*[NSEWO]?\s*$' THEN
            ( NULLIF(REPLACE(TRIM(regexp_replace(r.longitud, '[^0-9\+\-.,]', '', 'g')), ',', '.'), '')::numeric )
            * CASE WHEN TRIM(r.longitud) ~* '[SWO]' THEN -1 ELSE 1 END

          -- DMS (paréntesis balanceados)
          WHEN TRIM(r.longitud) ~* '^\s*\d+(?:\s*[°º]\s*\d+)?(?:\s*[''’′m]\s*\d+(?:[.,]\d+)?)?(?:\s*["”″s])?\s*[NSEWO]?\s*$' THEN
            (
              COALESCE((regexp_match(TRIM(r.longitud),
                '^\s*(\d+)(?:\s*[°º]\s*(\d+))?(?:\s*[''’′m]\s*(\d+(?:[.,]\d+)?))?(?:\s*["”″s])?\s*([NnSsEeWwOo])?\s*$'
              ))[1], '0')::numeric
              + COALESCE((regexp_match(TRIM(r.longitud),
                '^\s*(\d+)(?:\s*[°º]\s*(\d+))?(?:\s*[''’′m]\s*(\d+(?:[.,]\d+)?))?(?:\s*["”″s])?\s*([NnSsEeWwOo])?\s*$'
              ))[2], '0')::numeric / 60
              + COALESCE(REPLACE(COALESCE((regexp_match(TRIM(r.longitud),
                '^\s*(\d+)(?:\s*[°º]\s*(\d+))?(?:\s*[''’′m]\s*(\d+(?:[.,]\d+)?))?(?:\s*["”″s])?\s*([NnSsEeWwOo])?\s*$'
              ))[3], '0'), ',', '.')::numeric, 0) / 3600
            ) * CASE
                  WHEN COALESCE((regexp_match(TRIM(r.longitud),
                    '^\s*(\d+)(?:\s*[°º]\s*(\d+))?(?:\s*[''’′m]\s*(\d+(?:[.,]\d+)?))?(?:\s*["”″s])?\s*([NnSsEeWwOo])?\s*$'
                  ))[4], 'E') ~* '^[SWO]$' THEN -1 ELSE 1
                END
          ELSE NULL
        END
      )::numeric(11,8) AS longitud,

      LEFT(NULLIF(TRIM(r.ubigeo),''), 10)                      AS ubigeo,
      LEFT(NULLIF(TRIM(r.departamento),''), 120)               AS departamento,
      LEFT(NULLIF(TRIM(r.provincia),''), 120)                  AS provincia,
      LEFT(NULLIF(TRIM(r.distrito),''), 120)                   AS distrito,
      LEFT(NULLIF(TRIM(r.tipo_local),''), 120)                 AS tipo_local,
      LEFT(NULLIF(TRIM(r.proveedor_flm),''), 150)              AS proveedor_flm,
      LEFT(NULLIF(TRIM(r.atencion),''), 120)                   AS atencion,
      LEFT(NULLIF(TRIM(r.zona),''), 120)                       AS zona,
      LEFT(NULLIF(TRIM(r.tipo_zona_flm),''), 120)              AS tipo_zona_flm,
      LEFT(NULLIF(TRIM(r.propietario_de_torre),''), 150)       AS propietario_de_torre,
      LEFT(NULLIF(TRIM(r.propietario_de_piso),''), 150)        AS propietario_de_piso,
      LEFT(NULLIF(TRIM(r.tipo_estacion),''), 120)              AS tipo_estacion,
      LEFT(NULLIF(TRIM(r.tipo_torre),''), 120)                 AS tipo_torre,
      LEFT(NULLIF(TRIM(r.clasificacion_instalacion),''), 150)  AS clasificacion_instalacion,
      CASE
        WHEN NULLIF(TRIM(r.fecha_puesta_en_servicio),'') IS NULL THEN NULL
        WHEN r.fecha_puesta_en_servicio ~ '^\d{4}-\d{2}-\d{2}$' THEN r.fecha_puesta_en_servicio::date
        WHEN r.fecha_puesta_en_servicio ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(r.fecha_puesta_en_servicio,'DD/MM/YYYY')
        WHEN r.fecha_puesta_en_servicio ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(r.fecha_puesta_en_servicio,'DD-MM-YYYY')
        ELSE NULL
      END                                                      AS fecha_puesta_en_servicio,
      LEFT(NULLIF(TRIM(r.bucket),''), 80)                      AS bucket,
      LEFT(NULLIF(TRIM(r.sla),''), 80)                         AS sla,
      LEFT(NULLIF(TRIM(r.tipo_vip),''), 80)                    AS tipo_vip,
      LEFT(NULLIF(TRIM(r.criticidad_local),''), 80)            AS criticidad_local,
      LEFT(NULLIF(TRIM(r.observacion),''), 500)                AS observacion,
      LEFT(NULLIF(TRIM(r.priorizacion),''), 80)                AS priorizacion,
      LEFT(NULLIF(TRIM(r.accion),''), 15)                      AS accion,
      LEFT(NULLIF(TRIM(r.solicitado_por),''), 120)             AS solicitado_por,
      COALESCE(LEFT(TRIM(r.base), 6), '')::char(6)             AS base,
      LEFT(NULLIF(TRIM(r.campo_modificado),''), 150)           AS campo_modificado,
      LEFT(NULLIF(TRIM(r.observaciones),''), 450)              AS observaciones,
      LEFT(NULLIF(TRIM(r.estado),''), 20)                      AS estado,

      -- auxiliar para orden por fecha (ya no se usa para DISTINCT, pero se mantiene por consistencia)
      CASE
        WHEN NULLIF(TRIM(r.fecha_puesta_en_servicio),'') IS NULL THEN NULL
        WHEN r.fecha_puesta_en_servicio ~ '^\d{4}-\d{2}-\d{2}$' THEN r.fecha_puesta_en_servicio::date
        WHEN r.fecha_puesta_en_servicio ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(r.fecha_puesta_en_servicio,'DD/MM/YYYY')
        WHEN r.fecha_puesta_en_servicio ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(r.fecha_puesta_en_servicio,'DD-MM-YYYY')
        ELSE NULL
      END                                                      AS _ord_fecha
    FROM raw.fs_mm_bitacora_base_sitios r
  ),
  -- 2) Ya no hay DISTINCT ON: insertamos TODO
  ins AS (
    INSERT INTO ods.fs_hm_bitacora_base_sitios (
      codigo_unico, nombre_local, direccion, latitud, longitud, ubigeo,
      departamento, provincia, distrito, tipo_local, proveedor_flm, atencion,
      zona, tipo_zona_flm, propietario_de_torre, propietario_de_piso,
      tipo_estacion, tipo_torre, clasificacion_instalacion,
      fecha_puesta_en_servicio, bucket, sla, tipo_vip, criticidad_local,
      observacion, priorizacion, accion, solicitado_por, base,
      campo_modificado, observaciones, estado
    )
    SELECT
      cu AS codigo_unico, nombre_local, direccion, latitud, longitud, ubigeo,
      departamento, provincia, distrito, tipo_local, proveedor_flm, atencion,
      zona, tipo_zona_flm, propietario_de_torre, propietario_de_piso,
      tipo_estacion, tipo_torre, clasificacion_instalacion,
      fecha_puesta_en_servicio, bucket, sla, tipo_vip, criticidad_local,
      observacion, priorizacion, accion, solicitado_por, base,
      campo_modificado, observaciones, estado
    FROM raw_clean
    RETURNING 1
  )
  SELECT COALESCE((SELECT COUNT(*) FROM ins), 0)
  INTO v_inserted;

  v_estado := 'DONE';
  v_msj := format('Insert fs_hm_bitacora_base_sitios: inserted=%s, deleted=%s', v_inserted, v_deleted);

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
    v_estado := 'ERROR';
    v_msj := SQLERRM;
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
    RAISE;
END;
$procedure$
;
