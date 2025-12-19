-- DROP PROCEDURE ods.sp_cargar_web_md_neteco();

CREATE OR REPLACE PROCEDURE ods.sp_cargar_web_md_neteco()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio    timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp     integer      := 'ods.sp_cargar_web_md_neteco()'::regprocedure::oid::int;
  v_sp_name   text         := 'ods.sp_cargar_web_md_neteco()'::regprocedure::text;

  v_inserted  integer := 0;
  v_updated   integer := 0;

  v_estado    varchar(50);
  v_msj       text;
BEGIN
  /* ========= 1) TRANSFORMACIÓN + AGRUPACIÓN (FUSIONA DUPLICADOS) ========= */
  CREATE TEMP TABLE tmp_web_md_neteco_dedup
  ON COMMIT DROP AS
  WITH base AS (
    SELECT
      r.ctid AS rid,

      -- Limpieza básica de strings
      NULLIF(NULLIF(btrim(r.site_name),       'NaN'),'')::text AS site_name,
      NULLIF(NULLIF(btrim(r.subnet),          'NaN'),'')::text AS subnet,
      NULLIF(NULLIF(btrim(r.manage_object),   'NaN'),'')::text AS manage_object,

      -- Start Time -> timestamp (descarta valores vacíos / N/A)
      CASE
        WHEN r.start_time IS NULL
          OR btrim(r.start_time) = ''
          OR lower(btrim(r.start_time)) IN ('nan','null','-','n/a')
        THEN NULL
        ELSE r.start_time::timestamp
      END AS start_time,

      -- Energy per hour
      public.try_numeric(
        CASE
          WHEN r.energy_consumption_per_hour_kwh IS NULL
            OR btrim(r.energy_consumption_per_hour_kwh) = ''
            OR lower(btrim(r.energy_consumption_per_hour_kwh)) IN ('nan','null','-','n/a')
          THEN NULL
          ELSE btrim(r.energy_consumption_per_hour_kwh)
        END
      )::numeric(18,4) AS energy_consumption_per_hour,

      -- Supply duration per hour (en tu ejemplo casi siempre '-')
      public.try_numeric(
        CASE
          WHEN r.supply_duration_per_hour_h IS NULL
            OR btrim(r.supply_duration_per_hour_h) = ''
            OR lower(btrim(r.supply_duration_per_hour_h)) IN ('nan','null','-','n/a')
          THEN NULL
          ELSE btrim(r.supply_duration_per_hour_h)
        END
      )::numeric(18,4) AS supply_duration_per_hour,

      -- Total energy consumption
      public.try_numeric(
        CASE
          WHEN r.total_energy_consumption_kwh IS NULL
            OR btrim(r.total_energy_consumption_kwh) = ''
            OR lower(btrim(r.total_energy_consumption_kwh)) IN ('nan','null','-','n/a')
          THEN NULL
          ELSE btrim(r.total_energy_consumption_kwh)
        END
      )::numeric(18,4) AS total_energy_consumption,

      NULLIF(NULLIF(btrim(r.archivo),'NaN'),'')::varchar(255) AS archivo,

      r.fecha_carga
    FROM raw.web_md_neteco r
  ),
  agg AS (
    /*
       PK lógica ODS: (site_name, manage_object, start_time).

       Si hay 2 filas (una con energy y otra con total), aquí se fusionan:
       - MAX() ignora NULL -> nos quedamos con el valor "bueno" si existe.
       OJO: agrupamos SOLO por la PK, no por subnet.
    */
    SELECT
      site_name,
      manage_object,
      start_time,

      MAX(subnet)                      AS subnet,
      MAX(energy_consumption_per_hour) AS energy_consumption_per_hour,
      MAX(supply_duration_per_hour)    AS supply_duration_per_hour,
      MAX(total_energy_consumption)    AS total_energy_consumption,
      MAX(archivo)                     AS archivo
    FROM base
    WHERE site_name     IS NOT NULL
      AND manage_object IS NOT NULL
      AND start_time    IS NOT NULL
    GROUP BY site_name, manage_object, start_time
  )
  SELECT
    site_name,
    subnet,
    manage_object,
    start_time,
    energy_consumption_per_hour,
    supply_duration_per_hour,
    total_energy_consumption,
    archivo
  FROM agg;

  /* ========= 2.a) UPDATE ODS (registros ya existentes) ========= */
  UPDATE ods.web_hd_neteco o
  SET
    subnet                     = t.subnet,
    energy_consumption_per_hour= t.energy_consumption_per_hour,
    supply_duration_per_hour   = t.supply_duration_per_hour,
    total_energy_consumption   = t.total_energy_consumption,
    archivo                    = t.archivo,
    fecha_ultima_actualizacion = clock_timestamp()
  FROM tmp_web_md_neteco_dedup t
  WHERE o.site_name     = t.site_name
    AND o.manage_object = t.manage_object
    AND o.start_time    = t.start_time;

  GET DIAGNOSTICS v_updated = ROW_COUNT;

  /* ========= 2.b) INSERT ODS (registros nuevos) ========= */
  INSERT INTO ods.web_hd_neteco (
    site_name,
    manage_object,
    start_time,
    subnet,
    energy_consumption_per_hour,
    supply_duration_per_hour,
    total_energy_consumption,
    archivo,
    fecha_ultima_actualizacion
  )
  SELECT
    t.site_name,
    t.manage_object,
    t.start_time,
    t.subnet,
    t.energy_consumption_per_hour,
    t.supply_duration_per_hour,
    t.total_energy_consumption,
    t.archivo,
    clock_timestamp()
  FROM tmp_web_md_neteco_dedup t
  LEFT JOIN ods.web_hd_neteco o
    ON o.site_name     = t.site_name
   AND o.manage_object = t.manage_object
   AND o.start_time    = t.start_time
  WHERE o.site_name IS NULL;

  GET DIAGNOSTICS v_inserted = ROW_COUNT;

  /* ========= 3) REFRESCO TABLA DIARIA (EXCLUYENDO DÍA ACTUAL) ========= */
  TRUNCATE TABLE ods.web_hd_neteco_diaria;

  INSERT INTO ods.web_hd_neteco_diaria (
    site_name,
    manage_object,
    subnet,
    fecha,
    energy_consumption_per_day_kwh,
    supply_duration_per_day_h,
    total_energy_consumption_per_day_kwh
  )
  SELECT
    site_name,
    manage_object,
    subnet,
    date_trunc('day', start_time)::date AS fecha,
    SUM(energy_consumption_per_hour)    AS energy_consumption_per_day_kwh,
    SUM(supply_duration_per_hour)       AS supply_duration_per_day_h,
    SUM(total_energy_consumption)       AS total_energy_consumption_per_day_kwh
  FROM ods.web_hd_neteco
  WHERE date_trunc('day', start_time)::date < CURRENT_DATE
  GROUP BY
    site_name,
    manage_object,
    subnet,
    date_trunc('day', start_time)::date;

  /* ========= 4) LOG ========= */
  v_estado := 'DONE';
  v_msj := format(
    'UPSERT en ods.web_hd_neteco -> Insertados: %s | Actualizados: %s | Refresco diario en ods.web_hd_neteco_diaria. Origen: raw.web_md_neteco.',
    COALESCE(v_inserted,0),
    COALESCE(v_updated,0)
  );

  CALL public.sp_grabar_log_sp(
    p_id_sp      => v_id_sp,
    p_inicio     => v_inicio,
    p_fin        => clock_timestamp()::timestamp(0),
    p_inserted   => v_inserted,
    p_updated    => v_updated,
    p_deleted    => NULL::integer,
    p_nulls      => NULL::integer,
    p_estado     => v_estado,
    p_msj_error  => v_msj,
    p_sp         => v_sp_name
  );

EXCEPTION
  WHEN OTHERS THEN
    v_estado := 'ERROR';
    v_msj    := SQLERRM;

    CALL public.sp_grabar_log_sp(
      p_id_sp      => v_id_sp,
      p_inicio     => v_inicio,
      p_fin        => clock_timestamp()::timestamp(0),
      p_inserted   => NULL::integer,
      p_updated    => NULL::integer,
      p_deleted    => NULL::integer,
      p_nulls      => NULL::integer,
      p_estado     => v_estado,
      p_msj_error  => v_msj,
      p_sp         => v_sp_name
    );

    RAISE;
END;
$procedure$
;
