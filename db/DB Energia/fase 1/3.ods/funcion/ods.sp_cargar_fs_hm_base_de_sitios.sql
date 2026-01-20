

CREATE OR REPLACE PROCEDURE ods.sp_cargar_fs_hm_base_de_sitios()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio    timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp     integer      := 'ods.sp_cargar_fs_hm_base_de_sitios()'::regprocedure::oid::int;
  v_sp_name   text         := 'ods.sp_cargar_fs_hm_base_de_sitios()'::regprocedure::text;

  v_inserted  integer := 0;
  v_updated   integer := 0;
  v_deleted   integer := 0;
  v_nulls     integer := NULL;

  v_estado    varchar(50);
  v_msj       text;
BEGIN
  /*
    Flujo:
      1) Contar filas actuales en ODS y TRUNCATE (full refresh).
      2) Limpiar RAW (raw_clean) sin colapsar ni filtrar cu.
      3) Insertar TODO raw_clean -> ODS.
      4) Log.
  */

  -- 1) Contamos antes de truncar para reportar cuántas se eliminaron
  SELECT COUNT(*) INTO v_deleted FROM ods.fs_hm_base_de_sitios;

  -- IMPORTANTE:
  --  - Si hay FKs a ods.fs_hm_base_de_sitios, usar: TRUNCATE TABLE ods.fs_hm_base_de_sitios CASCADE;
  --  - Si necesitas reiniciar identidades: TRUNCATE TABLE ods.fs_hm_base_de_sitios RESTART IDENTITY;
  TRUNCATE TABLE ods.fs_hm_base_de_sitios;

  WITH
  raw_clean AS (
    SELECT
      TRIM(r.codigo_unico)                                   AS cu,           -- NO se filtra; conserva nulos/vacíos si vienen así
      LEFT(NULLIF(TRIM(r.nombre_local),''), 200)             AS nombre_local,
      LEFT(NULLIF(TRIM(r.direccion),''), 350)                AS direccion,

      -- Lat/Lon solo decimales (con coma o punto). Si no cumple patrón → NULL
      CASE
        WHEN TRIM(r.latitud)  ~ '^\s*[+-]?\d+(?:[.,]\d+)?\s*$'
          THEN REPLACE(TRIM(r.latitud), ',', '.')::numeric(11,8)
        ELSE NULL
      END                                                    AS latitud,
      CASE
        WHEN TRIM(r.longitud) ~ '^\s*[+-]?\d+(?:[.,]\d+)?\s*$'
          THEN REPLACE(TRIM(r.longitud), ',', '.')::numeric(11,8)
        ELSE NULL
      END                                                    AS longitud,

      LEFT(NULLIF(TRIM(r.ubigeo),''), 10)                    AS ubigeo,
      LEFT(NULLIF(TRIM(r.departamento),''), 120)             AS departamento,
      LEFT(NULLIF(TRIM(r.provincia),''), 120)                AS provincia,
      LEFT(NULLIF(TRIM(r.distrito),''), 120)                 AS distrito,
      LEFT(NULLIF(TRIM(r.tipo_local),''), 120)               AS tipo_local,
      LEFT(NULLIF(TRIM(r.proveedor_flm),''), 150)            AS proveedor_flm,
      LEFT(NULLIF(TRIM(r.atencion),''), 120)                 AS atencion,
      LEFT(NULLIF(TRIM(r.zona),''), 120)                     AS zona,
      LEFT(NULLIF(TRIM(r.tipo_zona_flm),''), 120)            AS tipo_zona_flm,
      LEFT(NULLIF(TRIM(r.propietario_de_torre),''), 150)     AS propietario_de_torre,
      LEFT(NULLIF(TRIM(r.propietario_de_piso),''), 150)      AS propietario_de_piso,
      LEFT(NULLIF(TRIM(r.tipo_estacion),''), 120)            AS tipo_estacion,
      LEFT(NULLIF(TRIM(r.tipo_torre),''), 120)               AS tipo_torre,
      LEFT(NULLIF(TRIM(r.clasificacion_instalacion),''),150) AS clasificacion_instalacion,
      CASE
        WHEN NULLIF(TRIM(r.fecha_puesta_en_servicio),'') IS NULL THEN NULL
        WHEN r.fecha_puesta_en_servicio ~ '^\d{4}-\d{2}-\d{2}$' THEN r.fecha_puesta_en_servicio::date
        WHEN r.fecha_puesta_en_servicio ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(r.fecha_puesta_en_servicio,'DD/MM/YYYY')
        WHEN r.fecha_puesta_en_servicio ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(r.fecha_puesta_en_servicio,'DD-MM-YYYY')
        ELSE NULL
      END                                                    AS fecha_puesta_en_servicio,
      LEFT(NULLIF(TRIM(r.bucket),''), 80)                    AS bucket,
      LEFT(NULLIF(TRIM(r.sla),''), 80)                       AS sla,
      LEFT(NULLIF(TRIM(r.tipo_vip),''), 80)                  AS tipo_vip,
      LEFT(NULLIF(TRIM(r.criticidad_local),''), 80)          AS criticidad_local,
      LEFT(NULLIF(TRIM(r.observacion),''), 500)              AS observacion,
      LEFT(NULLIF(TRIM(r.priorizacion),''), 80)              AS priorizacion,
      LEFT(NULLIF(TRIM(r.ubigeotoa),''), 120)                AS ubigeotoa
    FROM raw.fs_mm_base_de_sitios r
  ),
  ins AS (
    INSERT INTO ods.fs_hm_base_de_sitios (
      codigo_unico, nombre_local, direccion, latitud, longitud, ubigeo,
      departamento, provincia, distrito, tipo_local, proveedor_flm, atencion,
      zona, tipo_zona_flm, propietario_de_torre, propietario_de_piso,
      tipo_estacion, tipo_torre, clasificacion_instalacion,
      fecha_puesta_en_servicio, bucket, sla, tipo_vip, criticidad_local,
      observacion, priorizacion, ubigeotoa
    )
    SELECT
      cu AS codigo_unico, nombre_local, direccion, latitud, longitud, ubigeo,
      departamento, provincia, distrito, tipo_local, proveedor_flm, atencion,
      zona, tipo_zona_flm, propietario_de_torre, propietario_de_piso,
      tipo_estacion, tipo_torre, clasificacion_instalacion,
      fecha_puesta_en_servicio, bucket, sla, tipo_vip, criticidad_local,
      observacion, priorizacion, ubigeotoa
    FROM raw_clean
    RETURNING 1
  )
  SELECT COALESCE((SELECT COUNT(*) FROM ins), 0)
  INTO v_inserted;

  v_estado := 'DONE';
  v_msj := format('Full refresh fs_hm_base_de_sitios: truncated=%s, inserted=%s', v_deleted, v_inserted);

  CALL public.sp_grabar_log_sp(
    p_id_sp      => v_id_sp,
    p_inicio     => v_inicio,
    p_fin        => clock_timestamp()::timestamp(0),
    p_inserted   => v_inserted,
    p_updated    => v_updated,   -- 0
    p_deleted    => v_deleted,   -- filas eliminadas por el TRUNCATE
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
