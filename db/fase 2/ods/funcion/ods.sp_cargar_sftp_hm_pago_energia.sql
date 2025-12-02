-- DROP PROCEDURE ods.sp_cargar_sftp_hm_pago_energia();

CREATE OR REPLACE PROCEDURE ods.sp_cargar_sftp_hm_pago_energia()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio        timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp         integer      := 'ods.sp_cargar_sftp_hm_pago_energia()'::regprocedure::oid::int;
  v_sp_name       text         := 'ods.sp_cargar_sftp_hm_pago_energia()'::regprocedure::text;

  v_inserted      integer := 0;
  v_updated       integer := 0;
  v_err_dups      integer := 0;
  v_err_nulos     integer := 0;
  v_err_total     integer := 0;

  v_estado        varchar(50);
  v_msj           text;
BEGIN
  /* ========= 1.a) NULOS / VACÍOS en RECIBO ========= */
  WITH cand AS (
    SELECT r.*
    FROM raw.sftp_mm_pago_energia r
    WHERE r.recibo IS NULL
       OR btrim(r.recibo) = ''
       OR lower(btrim(r.recibo)) IN ('nan','null')
  )
  INSERT INTO public.error_pago_energia (
    nota_al_aprobador_char_40,
    concesionario,
    ruc_concesionario,
    numero_suministro,
    recibo,
    total_pagado,
    fecha_emision,
    fecha_vencimiento,
    periodo_consumo,
    archivo,
    fecha_carga,
    tabla_origen
  )
  SELECT
    r.nota_al_aprobador_char_40,
    r.concesionario,
    r.ruc_concesionario,
    r.numero_suministro,
    r.recibo,
    r.total_pagado,
    r.fecha_emision,
    r.fecha_vencimiento,
    r.periodo_consumo,
    r.archivo,
    clock_timestamp(),
    'raw.sftp_mm_pago_energia'
  FROM cand r
  LEFT JOIN public.error_pago_energia e
         ON ( (e.recibo IS NULL AND r.recibo IS NULL) OR e.recibo = r.recibo )
        AND e.tabla_origen = 'raw.sftp_mm_pago_energia'
        AND e.fecha_carga::date = clock_timestamp()::date
  WHERE e.fecha_carga IS NULL;

  GET DIAGNOSTICS v_err_nulos = ROW_COUNT;

  -- Borro NULOS/VACÍOS de RAW
  WITH rid_nulos AS (
    SELECT r.ctid AS rid
    FROM raw.sftp_mm_pago_energia r
    WHERE r.recibo IS NULL
       OR btrim(r.recibo) = ''
       OR lower(btrim(r.recibo)) IN ('nan','null')
  )
  DELETE FROM raw.sftp_mm_pago_energia r
  USING rid_nulos x
  WHERE r.ctid = x.rid;

  /* ========= 1.b) DUPLICADOS EXACTOS por RECIBO ========= */
  WITH claves_dup AS (
    SELECT r.recibo
    FROM raw.sftp_mm_pago_energia r
    GROUP BY r.recibo
    HAVING COUNT(*) > 1
  ),
  cand_dups AS (
    SELECT r.*
    FROM raw.sftp_mm_pago_energia r
    JOIN claves_dup d ON d.recibo = r.recibo
  )
  INSERT INTO public.error_pago_energia (
    nota_al_aprobador_char_40,
    concesionario,
    ruc_concesionario,
    numero_suministro,
    recibo,
    total_pagado,
    fecha_emision,
    fecha_vencimiento,
    periodo_consumo,
    archivo,
    fecha_carga,
    tabla_origen
  )
  SELECT
    r.nota_al_aprobador_char_40,
    r.concesionario,
    r.ruc_concesionario,
    r.numero_suministro,
    r.recibo,
    r.total_pagado,
    r.fecha_emision,
    r.fecha_vencimiento,
    r.periodo_consumo,
    r.archivo,
    clock_timestamp(),
    'raw.sftp_mm_pago_energia'
  FROM cand_dups r
  LEFT JOIN public.error_pago_energia e
         ON e.recibo = r.recibo
        AND e.tabla_origen = 'raw.sftp_mm_pago_energia'
        AND e.fecha_carga::date = clock_timestamp()::date
  WHERE e.recibo IS NULL;

  GET DIAGNOSTICS v_err_dups = ROW_COUNT;

  -- Borro duplicados por RECIBO de RAW (se envían sólo a error)
  WITH claves_dup AS (
    SELECT r.recibo
    FROM raw.sftp_mm_pago_energia r
    GROUP BY r.recibo
    HAVING COUNT(*) > 1
  ),
  rid_dup AS (
    SELECT r.ctid AS rid
    FROM raw.sftp_mm_pago_energia r
    JOIN claves_dup d ON d.recibo = r.recibo
  )
  DELETE FROM raw.sftp_mm_pago_energia r
  USING rid_dup x
  WHERE r.ctid = x.rid;

  v_err_total := COALESCE(v_err_dups,0) + COALESCE(v_err_nulos,0);

  /* ========= 2) TRANSFORMACIÓN + UPSERT A ODS ========= */
  CREATE TEMP TABLE tmp_sftp_pago_energia_dedup
  ON COMMIT DROP AS
  WITH base AS (
    SELECT
      r.ctid AS rid,

      NULLIF(NULLIF(btrim(r.nota_al_aprobador_char_40),'NaN'),'')::varchar(250)
        AS nota_al_aprobador_char_40,
      NULLIF(NULLIF(btrim(r.concesionario),'NaN'),'')::varchar(255)
        AS concesionario,
      trunc(public.try_numeric(r.ruc_concesionario))::bigint
        AS ruc_concesionario,
      NULLIF(NULLIF(btrim(r.numero_suministro),'NaN'),'')::varchar(255)
        AS numero_suministro,

      CASE
        WHEN r.recibo IS NULL
          OR btrim(r.recibo) = ''
          OR lower(btrim(r.recibo)) IN ('nan','null')
        THEN NULL
        ELSE left(btrim(r.recibo), 255)
      END::varchar(255) AS recibo,

      public.try_numeric(r.total_pagado)::numeric(18,2)
        AS total_pagado,

      CASE
        WHEN r.fecha_emision IS NULL
          OR btrim(r.fecha_emision) = ''
          OR lower(btrim(r.fecha_emision)) IN ('nan','null')
        THEN NULL
        WHEN position('/' in r.fecha_emision) > 0
          THEN to_date(split_part(r.fecha_emision,' ',1), 'DD/MM/YYYY')
        ELSE to_date(split_part(r.fecha_emision,' ',1), 'YYYY-MM-DD')
      END AS fecha_emision,

      CASE
        WHEN r.fecha_vencimiento IS NULL
          OR btrim(r.fecha_vencimiento) = ''
          OR lower(btrim(r.fecha_vencimiento)) IN ('nan','null')
        THEN NULL
        WHEN position('/' in r.fecha_vencimiento) > 0
          THEN to_date(split_part(r.fecha_vencimiento,' ',1), 'DD/MM/YYYY')
        ELSE to_date(split_part(r.fecha_vencimiento,' ',1), 'YYYY-MM-DD')
      END AS fecha_vencimiento,

      NULLIF(NULLIF(btrim(r.periodo_consumo),'NaN'),'')::varchar(255)
        AS periodo_consumo,

      NULLIF(NULLIF(btrim(r.archivo),'NaN'),'')::varchar(255)
        AS archivo

    FROM raw.sftp_mm_pago_energia r
  ),
  dedup AS (
    -- PK lógica ODS: recibo (conserva primera aparición)
    SELECT DISTINCT ON (recibo) *
    FROM base
    WHERE recibo IS NOT NULL
    ORDER BY recibo, rid
  )
  SELECT
    nota_al_aprobador_char_40,
    concesionario,
    ruc_concesionario,
    numero_suministro,
    recibo,
    total_pagado,
    fecha_emision,
    fecha_vencimiento,
    periodo_consumo,
    archivo
  FROM dedup;

  -- ========= 2.a) UPDATE (parte UPDATE del UPSERT) =========
  UPDATE ods.sftp_hm_pago_energia o
  SET
    nota_al_aprobador_char_40 = t.nota_al_aprobador_char_40,
    concesionario             = t.concescionario,
    ruc_concesionario         = t.ruc_concesionario,
    numero_suministro         = t.numero_suministro,
    total_pagado              = t.total_pagado,
    fecha_emision             = t.fecha_emision,
    fecha_vencimiento         = t.fecha_vencimiento,
    periodo_consumo           = t.periodo_consumo,
    archivo                   = t.archivo
  FROM tmp_sftp_pago_energia_dedup t
  WHERE o.recibo = t.recibo;

  GET DIAGNOSTICS v_updated = ROW_COUNT;

  -- ========= 2.b) INSERT (parte INSERT del UPSERT) =========
  INSERT INTO ods.sftp_hm_pago_energia (
    nota_al_aprobador_char_40,
    concesionario,
    ruc_concesionario,
    numero_suministro,
    recibo,
    total_pagado,
    fecha_emision,
    fecha_vencimiento,
    periodo_consumo,
    archivo
  )
  SELECT
    t.nota_al_aprobador_char_40,
    t.concesionario,
    t.ruc_concesionario,
    t.numero_suministro,
    t.recibo,
    t.total_pagado,
    t.fecha_emision,
    t.fecha_vencimiento,
    t.periodo_consumo,
    t.archivo
  FROM tmp_sftp_pago_energia_dedup t
  LEFT JOIN ods.sftp_hm_pago_energia o
    ON o.recibo = t.recibo
  WHERE o.recibo IS NULL;

  GET DIAGNOSTICS v_inserted = ROW_COUNT;

  /* ========= 3) LOG ========= */
  v_estado := 'DONE';
  v_msj := format(
    'UPSERT en ODS.sftp_hm_pago_energia -> Insertados: %s | Actualizados: %s | Enviados a public.error_pago_energia (hoy): %s (duplicados recibo: %s, nulos/vacíos recibo: %s) | Origen: raw.sftp_mm_pago_energia.',
    COALESCE(v_inserted,0),
    COALESCE(v_updated,0),
    COALESCE(v_err_total,0),
    COALESCE(v_err_dups,0),
    COALESCE(v_err_nulos,0)
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
