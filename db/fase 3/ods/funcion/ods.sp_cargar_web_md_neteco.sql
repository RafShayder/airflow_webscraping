
CREATE OR REPLACE PROCEDURE ods.sp_cargar_web_md_neteco()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio   timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp    integer      := 'ods.sp_cargar_web_md_neteco()'::regprocedure::oid::int;
  v_sp_name  text         := 'ods.sp_cargar_web_md_neteco()'::regprocedure::text;

  v_ins_dia  integer := 0;
  v_upd_dia  integer := 0;
  v_ins_mes  integer := 0;
  v_upd_mes  integer := 0;

  v_estado   varchar(50);
  v_msj      text;
BEGIN
  /* ========= 1) LIMPIEZA + DEDUP POR HORA (TEMP) ========= */
  CREATE TEMP TABLE tmp_neteco_hora_dedup
  ON COMMIT DROP AS
  WITH base AS (
    SELECT
      NULLIF(NULLIF(btrim(r.site_name), 'NaN'), '')::text AS site_name,
      NULLIF(NULLIF(btrim(r.subnet), 'NaN'), '')::text AS subnet,
      NULLIF(NULLIF(btrim(r.manage_object), 'NaN'), '')::text AS manage_object,

      CASE
        WHEN r.start_time IS NULL
          OR btrim(r.start_time) = ''
          OR lower(btrim(r.start_time)) IN ('nan','null','-','n/a')
        THEN NULL
        ELSE r.start_time::timestamp
      END AS start_time,

      public.try_numeric(
        CASE
          WHEN r.energy_consumption_per_hour_kwh IS NULL
            OR btrim(r.energy_consumption_per_hour_kwh) = ''
            OR lower(btrim(r.energy_consumption_per_hour_kwh)) IN ('nan','null','-','n/a')
          THEN NULL
          ELSE btrim(r.energy_consumption_per_hour_kwh)
        END
      )::numeric(18,4) AS energy_consumption_per_hour,

      public.try_numeric(
        CASE
          WHEN r.supply_duration_per_hour_h IS NULL
            OR btrim(r.supply_duration_per_hour_h) = ''
            OR lower(btrim(r.supply_duration_per_hour_h)) IN ('nan','null','-','n/a')
          THEN NULL
          ELSE btrim(r.supply_duration_per_hour_h)
        END
      )::numeric(18,4) AS supply_duration_per_hour,

      public.try_numeric(
        CASE
          WHEN r.total_energy_consumption_kwh IS NULL
            OR btrim(r.total_energy_consumption_kwh) = ''
            OR lower(btrim(r.total_energy_consumption_kwh)) IN ('nan','null','-','n/a')
          THEN NULL
          ELSE btrim(r.total_energy_consumption_kwh)
        END
      )::numeric(18,4) AS total_energy_consumption
    FROM raw.web_md_neteco r
  )
  SELECT
    site_name,
    manage_object,
    start_time,
    MAX(subnet)                      AS subnet,
    MAX(energy_consumption_per_hour) AS energy_consumption_per_hour,
    MAX(supply_duration_per_hour)    AS supply_duration_per_hour,
    MAX(total_energy_consumption)    AS total_energy_consumption
  FROM base
  WHERE site_name IS NOT NULL
    AND manage_object IS NOT NULL
    AND start_time IS NOT NULL
  GROUP BY site_name, manage_object, start_time;

  /* ========= 2) UPSERT DIARIO (SIN TRUNCATE) ========= */
  WITH daily AS (
    SELECT
      site_name,
      manage_object,
      start_time::date AS fecha,
      MAX(subnet) AS subnet,
      SUM(energy_consumption_per_hour) AS energy_consumption_per_day_kwh,
      SUM(supply_duration_per_hour)    AS supply_duration_per_day_h,
      SUM(total_energy_consumption)    AS total_energy_consumption_per_day_kwh
    FROM tmp_neteco_hora_dedup
    WHERE start_time::date <= CURRENT_DATE   -- cambia a < CURRENT_DATE si quieres excluir hoy
    GROUP BY site_name, manage_object, start_time::date
  ),
  upsert_diaria AS (
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
      fecha,
      energy_consumption_per_day_kwh,
      supply_duration_per_day_h,
      total_energy_consumption_per_day_kwh
    FROM daily
    ON CONFLICT (site_name, manage_object, fecha)
    DO UPDATE SET
      subnet = EXCLUDED.subnet,
      energy_consumption_per_day_kwh = EXCLUDED.energy_consumption_per_day_kwh,
      supply_duration_per_day_h      = EXCLUDED.supply_duration_per_day_h,
      total_energy_consumption_per_day_kwh = EXCLUDED.total_energy_consumption_per_day_kwh
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    COALESCE(COUNT(*) FILTER (WHERE inserted), 0),
    COALESCE(COUNT(*) FILTER (WHERE NOT inserted), 0)
  INTO v_ins_dia, v_upd_dia
  FROM upsert_diaria;

  /* ========= 3) UPSERT MENSUAL (SIN TRUNCATE) ========= */
  WITH monthly AS (
    SELECT
      site_name,
      manage_object,
      date_trunc('month', start_time)::date AS mes, -- cumple CHECK day=1
      MAX(subnet) AS subnet,
      SUM(energy_consumption_per_hour) AS energy_consumption_per_month_kwh,
      SUM(supply_duration_per_hour)    AS supply_duration_per_month_h,
      SUM(total_energy_consumption)    AS total_energy_consumption_per_month_kwh
    FROM tmp_neteco_hora_dedup
    WHERE start_time::date <= CURRENT_DATE   -- cambia a < CURRENT_DATE si quieres excluir hoy
    GROUP BY site_name, manage_object, date_trunc('month', start_time)::date
  ),
  upsert_mensual AS (
    INSERT INTO ods.web_hd_neteco_mensual (
      site_name,
      manage_object,
      subnet,
      mes,
      energy_consumption_per_month_kwh,
      supply_duration_per_month_h,
      total_energy_consumption_per_month_kwh
    )
    SELECT
      site_name,
      manage_object,
      subnet,
      mes,
      energy_consumption_per_month_kwh,
      supply_duration_per_month_h,
      total_energy_consumption_per_month_kwh
    FROM monthly
    ON CONFLICT (site_name, manage_object, mes)
    DO UPDATE SET
      subnet = EXCLUDED.subnet,
      energy_consumption_per_month_kwh = EXCLUDED.energy_consumption_per_month_kwh,
      supply_duration_per_month_h      = EXCLUDED.supply_duration_per_month_h,
      total_energy_consumption_per_month_kwh = EXCLUDED.total_energy_consumption_per_month_kwh
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    COALESCE(COUNT(*) FILTER (WHERE inserted), 0),
    COALESCE(COUNT(*) FILTER (WHERE NOT inserted), 0)
  INTO v_ins_mes, v_upd_mes
  FROM upsert_mensual;

  /* ========= 4) LOG ========= */
  v_estado := 'DONE';
  v_msj := format(
    'RAW -> DIARIA/MENSUAL (UPSERT). Diaria: Insert=%s Update=%s | Mensual: Insert=%s Update=%s | Origen: raw.web_md_neteco.',
    COALESCE(v_ins_dia,0), COALESCE(v_upd_dia,0),
    COALESCE(v_ins_mes,0), COALESCE(v_upd_mes,0)
  );

  CALL public.sp_grabar_log_sp(
    p_id_sp      => v_id_sp,
    p_inicio     => v_inicio,
    p_fin        => clock_timestamp()::timestamp(0),
    p_inserted   => (COALESCE(v_ins_dia,0) + COALESCE(v_ins_mes,0)),
    p_updated    => (COALESCE(v_upd_dia,0) + COALESCE(v_upd_mes,0)),
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