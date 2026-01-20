
CREATE OR REPLACE PROCEDURE ods.sp_cargar_sftp_hd_toa()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio    timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp     integer      := 'ods.sp_cargar_sftp_hd_toa()'::regprocedure::oid::int;
  v_sp_name   text         := 'ods.sp_cargar_sftp_hd_toa()'::regprocedure::text;

  v_inserted  integer := 0;
  v_updated   integer := 0;
  v_deleted   integer := 0;
  v_nulls     integer := 0;

  v_estado    varchar(50);
  v_msj       text;
BEGIN
  -- 1) Contamos filas actuales en destino y filas sin PK en origen
  SELECT COUNT(*) INTO v_deleted
  FROM ods.sftp_hd_toa;

  SELECT COUNT(*) INTO v_nulls
  FROM raw.sftp_hd_toa r
  WHERE r.nro_toa IS NULL OR btrim(r.nro_toa) = '';

  -- 2) Full refresh de la tabla destino
  TRUNCATE TABLE ods.sftp_hd_toa;

  WITH raw_clean AS (
    SELECT
      NULLIF(btrim(r.tecnico), '') AS tecnico,

      CASE
        WHEN btrim(r.id_recurso) ~ '^[0-9]+$'
          THEN btrim(r.id_recurso)::int
        ELSE NULL
      END AS id_recurso,

      NULLIF(btrim(r.nro_toa), '') AS nro_toa,
      NULLIF(btrim(r.subtipo_de_actividad), '') AS subtipo_de_actividad,
      NULLIF(btrim(r.numero_de_peticion), '') AS numero_de_peticion,

      /* ===== fecha_de_cita (DATE) ===== */
      CASE
        WHEN NULLIF(btrim(r.fecha_de_cita), '') IS NULL THEN NULL
        WHEN btrim(r.fecha_de_cita) ~ '^\d{4}-\d{2}-\d{2}$'
          THEN btrim(r.fecha_de_cita)::date
        WHEN btrim(r.fecha_de_cita) ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(btrim(r.fecha_de_cita), 'DD/MM/YYYY')
        WHEN btrim(r.fecha_de_cita) ~ '^\d{2}/\d{2}/\d{2}$'
          THEN to_date(btrim(r.fecha_de_cita), 'DD/MM/YY')
        ELSE NULL
      END AS fecha_de_cita,

      /* ===== TIMESTAMP genéricos (distintos formatos) ===== */

      -- sla_inicio
      CASE
        WHEN NULLIF(btrim(r.sla_inicio), '') IS NULL THEN NULL
        WHEN btrim(r.sla_inicio) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.sla_inicio)::timestamp
        WHEN btrim(r.sla_inicio) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.sla_inicio), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.sla_inicio) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.sla_inicio)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS sla_inicio,

      -- sla_fin
      CASE
        WHEN NULLIF(btrim(r.sla_fin), '') IS NULL THEN NULL
        WHEN btrim(r.sla_fin) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.sla_fin)::timestamp
        WHEN btrim(r.sla_fin) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.sla_fin), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.sla_fin) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.sla_fin)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS sla_fin,

      NULLIF(btrim(r.localidad), '') AS localidad,
      NULLIF(btrim(r.direccion), '') AS direccion,

      -- direccion_polar_x (NUMERIC)
      CASE
        WHEN NULLIF(btrim(r.direccion_polar_x), '') IS NULL THEN NULL
        WHEN btrim(r.direccion_polar_x) ~ '^[+-]?[0-9]+([.,][0-9]+)?$'
          THEN REPLACE(btrim(r.direccion_polar_x), ',', '.')::numeric
        ELSE NULL
      END AS direccion_polar_x,

      -- direccion_polar_y (NUMERIC)
      CASE
        WHEN NULLIF(btrim(r.direccion_polar_y), '') IS NULL THEN NULL
        WHEN btrim(r.direccion_polar_y) ~ '^[+-]?[0-9]+([.,][0-9]+)?$'
          THEN REPLACE(btrim(r.direccion_polar_y), ',', '.')::numeric
        ELSE NULL
      END AS direccion_polar_y,

      NULLIF(btrim(r.nombre_cliente), '') AS nombre_cliente,

      -- hora_de_asignacion_de_actividad
      CASE
        WHEN NULLIF(btrim(r.hora_de_asignacion_de_actividad), '') IS NULL THEN NULL
        WHEN btrim(r.hora_de_asignacion_de_actividad) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.hora_de_asignacion_de_actividad)::timestamp
        WHEN btrim(r.hora_de_asignacion_de_actividad) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.hora_de_asignacion_de_actividad), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.hora_de_asignacion_de_actividad) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.hora_de_asignacion_de_actividad)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS hora_de_asignacion_de_actividad,

      -- fecha_de_registro_de_actividad_toa
      CASE
        WHEN NULLIF(btrim(r.fecha_de_registro_de_actividad_toa), '') IS NULL THEN NULL
        WHEN btrim(r.fecha_de_registro_de_actividad_toa) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fecha_de_registro_de_actividad_toa)::timestamp
        WHEN btrim(r.fecha_de_registro_de_actividad_toa) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fecha_de_registro_de_actividad_toa), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fecha_de_registro_de_actividad_toa) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fecha_de_registro_de_actividad_toa)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fecha_de_registro_de_actividad_toa,

      NULLIF(btrim(r.notas), '') AS notas,
      NULLIF(btrim(r.codigo_de_cliente), '') AS codigo_de_cliente,

      -- fecha_hora_de_cancelacion
      CASE
        WHEN NULLIF(btrim(r.fecha_hora_de_cancelacion), '') IS NULL THEN NULL
        WHEN btrim(r.fecha_hora_de_cancelacion) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fecha_hora_de_cancelacion)::timestamp
        WHEN btrim(r.fecha_hora_de_cancelacion) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fecha_hora_de_cancelacion), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fecha_hora_de_cancelacion) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fecha_hora_de_cancelacion)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fecha_hora_de_cancelacion,

      NULLIF(btrim(r.empresa), '') AS empresa,
      NULLIF(btrim(r.bucket_inicial), '') AS bucket_inicial,
      NULLIF(btrim(r.usuario_iniciado), '') AS usuario_iniciado,
      NULLIF(btrim(r.nombre_distrito), '') AS nombre_distrito,
      NULLIF(btrim(r.sistema_origen), '') AS sistema_origen,
      NULLIF(btrim(r.id_del_ticket), '') AS id_del_ticket,
      NULLIF(btrim(r.quiebres), '') AS quiebres,

      -- fecha_de_inicio_pint
      CASE
        WHEN NULLIF(btrim(r.fecha_de_inicio_pint), '') IS NULL THEN NULL
        WHEN btrim(r.fecha_de_inicio_pint) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fecha_de_inicio_pint)::timestamp
        WHEN btrim(r.fecha_de_inicio_pint) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fecha_de_inicio_pint), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fecha_de_inicio_pint) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fecha_de_inicio_pint)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fecha_de_inicio_pint,

      -- inicio_pr1
      CASE
        WHEN NULLIF(btrim(r.inicio_pr1), '') IS NULL THEN NULL
        WHEN btrim(r.inicio_pr1) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.inicio_pr1)::timestamp
        WHEN btrim(r.inicio_pr1) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.inicio_pr1), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.inicio_pr1) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.inicio_pr1)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS inicio_pr1,

      -- fin_pr1
      CASE
        WHEN NULLIF(btrim(r.fin_pr1), '') IS NULL THEN NULL
        WHEN btrim(r.fin_pr1) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fin_pr1)::timestamp
        WHEN btrim(r.fin_pr1) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fin_pr1), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fin_pr1) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fin_pr1)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fin_pr1,

      -- fin_pr2
      CASE
        WHEN NULLIF(btrim(r.fin_pr2), '') IS NULL THEN NULL
        WHEN btrim(r.fin_pr2) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fin_pr2)::timestamp
        WHEN btrim(r.fin_pr2) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fin_pr2), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fin_pr2) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fin_pr2)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fin_pr2,

      -- inicio_pr2
      CASE
        WHEN NULLIF(btrim(r.inicio_pr2), '') IS NULL THEN NULL
        WHEN btrim(r.inicio_pr2) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.inicio_pr2)::timestamp
        WHEN btrim(r.inicio_pr2) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.inicio_pr2), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.inicio_pr2) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.inicio_pr2)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS inicio_pr2,

      -- fin_pr3
      CASE
        WHEN NULLIF(btrim(r.fin_pr3), '') IS NULL THEN NULL
        WHEN btrim(r.fin_pr3) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fin_pr3)::timestamp
        WHEN btrim(r.fin_pr3) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fin_pr3), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fin_pr3) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fin_pr3)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fin_pr3,

      -- inicio_pr3
      CASE
        WHEN NULLIF(btrim(r.inicio_pr3), '') IS NULL THEN NULL
        WHEN btrim(r.inicio_pr3) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.inicio_pr3)::timestamp
        WHEN btrim(r.inicio_pr3) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.inicio_pr3), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.inicio_pr3) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.inicio_pr3)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS inicio_pr3,

      -- fin_pr4
      CASE
        WHEN NULLIF(btrim(r.fin_pr4), '') IS NULL THEN NULL
        WHEN btrim(r.fin_pr4) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.fin_pr4)::timestamp
        WHEN btrim(r.fin_pr4) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.fin_pr4), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.fin_pr4) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.fin_pr4)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS fin_pr4,

      -- inicio_pr4
      CASE
        WHEN NULLIF(btrim(r.inicio_pr4), '') IS NULL THEN NULL
        WHEN btrim(r.inicio_pr4) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN btrim(r.inicio_pr4)::timestamp
        WHEN btrim(r.inicio_pr4) ~ '^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?$'
          THEN to_timestamp(btrim(r.inicio_pr4), 'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(r.inicio_pr4) ~ '^\d{2}/\d{2}/\d{2}\s+\d{1,2}:\d{2}\s*(AM|PM|am|pm)$'
          THEN to_timestamp(upper(btrim(r.inicio_pr4)), 'DD/MM/YY HH12:MI AM')
        ELSE NULL
      END AS inicio_pr4,

      NULLIF(btrim(r.motivo_pr1), '') AS motivo_pr1,
      NULLIF(btrim(r.motivo_pr2), '') AS motivo_pr2,
      NULLIF(btrim(r.motivo_pr3), '') AS motivo_pr3,
      NULLIF(btrim(r.motivo_pr4), '') AS motivo_pr4,
      NULLIF(btrim(r.nombre_local), '') AS nombre_local,
      NULLIF(btrim(r.tipo_de_local), '') AS tipo_de_local,
      NULLIF(btrim(r.zona_geografica), '') AS zona_geografica,
      NULLIF(btrim(r.zona), '') AS zona,
      NULLIF(btrim(r.estado_toa), '') AS estado_toa,
      NULLIF(btrim(r.duracion_pr1), '') AS duracion_pr1,
      NULLIF(btrim(r.duracion_pr2), '') AS duracion_pr2,
      NULLIF(btrim(r.duracion_pr3), '') AS duracion_pr3,
      NULLIF(btrim(r.duracion_pr4), '') AS duracion_pr4,
      NULLIF(btrim(r.estado), '') AS estado,
      NULLIF(btrim(r.codigo_completado_reparacion_y_preventivo_cable), '') AS codigo_completado_reparacion_y_preventivo_cable,
      NULLIF(btrim(r.cliente_acepta_solucion_anticipada_de_reclamo), '') AS cliente_acepta_solucion_anticipada_de_reclamo,
      NULLIF(btrim(r.averia_efectiva_marca_sar_t), '') AS averia_efectiva_marca_sar_t
    FROM raw.sftp_hd_toa r
  ),
  dedup AS (
    SELECT *
    FROM (
      SELECT
        rc.*,
        ROW_NUMBER() OVER (
          PARTITION BY rc.nro_toa
          ORDER BY rc.fecha_de_registro_de_actividad_toa DESC NULLS LAST
        ) AS rn
      FROM raw_clean rc
    ) t
    WHERE rn = 1
  ),
  ins AS (
    INSERT INTO ods.sftp_hd_toa (
      tecnico,
      id_recurso,
      nro_toa,
      subtipo_de_actividad,
      numero_de_peticion,
      fecha_de_cita,
      sla_inicio,
      sla_fin,
      localidad,
      direccion,
      direccion_polar_x,
      direccion_polar_y,
      nombre_cliente,
      hora_de_asignacion_de_actividad,
      fecha_de_registro_de_actividad_toa,
      notas,
      codigo_de_cliente,
      fecha_hora_de_cancelacion,
      empresa,
      bucket_inicial,
      usuario_iniciado,
      nombre_distrito,
      sistema_origen,
      id_del_ticket,
      quiebres,
      fecha_de_inicio_pint,
      inicio_pr1,
      fin_pr1,
      fin_pr2,
      inicio_pr2,
      fin_pr3,
      inicio_pr3,
      fin_pr4,
      inicio_pr4,
      motivo_pr1,
      motivo_pr2,
      motivo_pr3,
      motivo_pr4,
      nombre_local,
      tipo_de_local,
      zona_geografica,
      zona,
      estado_toa,
      duracion_pr1,
      duracion_pr2,
      duracion_pr3,
      duracion_pr4,
      estado,
      codigo_completado_reparacion_y_preventivo_cable,
      cliente_acepta_solucion_anticipada_de_reclamo,
      averia_efectiva_marca_sar_t
    )
    SELECT
      tecnico,
      id_recurso,
      nro_toa,
      subtipo_de_actividad,
      numero_de_peticion,
      fecha_de_cita,
      sla_inicio,
      sla_fin,
      localidad,
      direccion,
      direccion_polar_x,
      direccion_polar_y,
      nombre_cliente,
      hora_de_asignacion_de_actividad,
      fecha_de_registro_de_actividad_toa,
      notas,
      codigo_de_cliente,
      fecha_hora_de_cancelacion,
      empresa,
      bucket_inicial,
      usuario_iniciado,
      nombre_distrito,
      sistema_origen,
      id_del_ticket,
      quiebres,
      fecha_de_inicio_pint,
      inicio_pr1,
      fin_pr1,
      fin_pr2,
      inicio_pr2,
      fin_pr3,
      inicio_pr3,
      fin_pr4,
      inicio_pr4,
      motivo_pr1,
      motivo_pr2,
      motivo_pr3,
      motivo_pr4,
      nombre_local,
      tipo_de_local,
      zona_geografica,
      zona,
      estado_toa,
      duracion_pr1,
      duracion_pr2,
      duracion_pr3,
      duracion_pr4,
      estado,
      codigo_completado_reparacion_y_preventivo_cable,
      cliente_acepta_solucion_anticipada_de_reclamo,
      averia_efectiva_marca_sar_t
    FROM dedup
    WHERE nro_toa IS NOT NULL
    RETURNING 1
  )
  SELECT COALESCE((SELECT COUNT(*) FROM ins), 0)
  INTO v_inserted;

  v_estado := 'DONE';
  v_msj := format(
    'Insert ods.sftp_hd_toa: inserted=%s, deleted=%s, null_nro_toa=%s',
    v_inserted, v_deleted, v_nulls
  );

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
