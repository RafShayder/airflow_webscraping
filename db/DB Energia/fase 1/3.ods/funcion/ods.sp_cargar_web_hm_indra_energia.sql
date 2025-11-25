-- DROP PROCEDURE ods.sp_cargar_web_hm_indra_energia();

CREATE OR REPLACE PROCEDURE ods.sp_cargar_web_hm_indra_energia()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio        timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp         integer      := 'ods.sp_cargar_web_hm_indra_energia()'::regprocedure::oid::int;
  v_sp_name       text         := 'ods.sp_cargar_web_hm_indra_energia()'::regprocedure::text;

  v_ins_err_dup   integer := 0;  -- duplicados insertados a error_energia
  v_del_dup_raw   integer := 0;  -- duplicados borrados de RAW
  v_inserted_ods  integer := 0;  -- insertados en ODS

  v_estado        varchar(50);
  v_msj           text;
BEGIN
  /* ===================== 1) DUPLICADOS (num_recibo, per_consumo) ===================== */
  WITH claves_dup AS (
    SELECT num_recibo, per_consumo
    FROM raw.web_mm_indra_energia
    GROUP BY num_recibo, per_consumo
    HAVING COUNT(*) > 1
  ),
  cand_dups AS (
    SELECT r.*
    FROM raw.web_mm_indra_energia r
    JOIN claves_dup d
      ON d.num_recibo = r.num_recibo
     AND ( (d.per_consumo IS NULL AND r.per_consumo IS NULL) OR d.per_consumo = r.per_consumo )
  )
  INSERT INTO public.error_energia (
    num_recibo, cod_suministro, tarifa, pot_contr, titularidad, distribuidor,
    fec_emision, fec_vencimiento, fec_lect_actual, fec_lect_anterior, per_consumo,
    ea_lec_act_hp, ea_lec_act_fp, ea_lec_act_tot, ea_lec_ant_hp, ea_lec_ant_fp, ea_lec_ant_tot,
    ea_consumo_hp, ea_consumo_fp, ea_consumo_tot, ea_pu_hp, ea_pu_fp, ea_prec_unit_tot,
    ea_importe_hp, ea_importe_fp, ea_importe_tot,
    pot_lect_act_hp, pot_lect_act_fp, pot_lect_ant_hp, pot_lect_ant_fp, pot_reg_hp, pot_reg_fp,
    pot_gen_fact, pot_gen_pu, pot_gen_importe, pot_dist_fact, pot_dist_pu, pot_dist_importe,
    pot_bt6_pu, pot_bt6_monto,
    er_lec_act, er_lec_ant, er_cons, er_fact, er_pu, er_importe,
    calif_cliente, dias_punta, horas_punta, factor_calif,
    cargo_fijo, mantenimiento, alumbrado, recup_energia, mora, reconexion, ajuste_tarifa,
    dist_factura, ajuste_alumb, otros_afectos, base_imponible, igv, tot_periodo,
    electrif_rural, comp_distribucion, comp_generadora, comp_interrup, comp_calidad,
    comp_frec_ant, comp_frec_des, comp_serv, comp_norma_tec, comp_deuda_ant, comp_deuda_meses,
    comp_dev_reclamo, comp_nota_deb_cred, comp_aporte_reemb, comp_otros_inafectos,
    comp_redondeo_act_pos, comp_redondeo_act_neg,
    costo_medio, total_a_pagar, valor_venta, deuda_anterior, devolucion, fecha_liquidacion,
    fecha_carga, tabla_origen
  )
  SELECT
    r.num_recibo, r.cod_suministro, r.tarifa,
    r.pot_contr,                       -- << si error_energia.pot_contr es NUMERIC, puedes usar public.try_numeric(r.pot_contr)
    r.titularidad, r.distribuidor,
    r.fec_emision, r.fec_vencimiento, r.fec_lect_actual, r.fec_lect_anterior, r.per_consumo,
    r.ea_lec_act_hp, r.ea_lec_act_fp, r.ea_lec_act_tot, r.ea_lec_ant_hp, r.ea_lec_ant_fp, r.ea_lec_ant_tot,
    r.ea_consumo_hp, r.ea_consumo_fp, r.ea_consumo_tot, r.ea_pu_hp, r.ea_pu_fp, r.ea_prec_unit_tot,
    r.ea_importe_hp, r.ea_importe_fp, r.ea_importe_tot,
    r.pot_lect_act_hp, r.pot_lect_act_fp, r.pot_lect_ant_hp, r.pot_lect_ant_fp, r.pot_reg_hp, r.pot_reg_fp,
    r.pot_gen_fact, r.pot_gen_pu, r.pot_gen_importe, r.pot_dist_fact, r.pot_dist_pu, r.pot_dist_importe,
    r.pot_bt6_pu, r.pot_bt6_monto,
    r.er_lec_act, r.er_lec_ant, r.er_cons, r.er_fact, r.er_pu, r.er_importe,
    r.calif_cliente, r.dias_punta, r.horas_punta, r.factor_calif,
    r.cargo_fijo, r.mantenimiento, r.alumbrado, r.recup_energia, r.mora, r.reconexion, r.ajuste_tarifa,
    r.dist_factura, r.ajuste_alumb, r.otros_afectos, r.base_imponible, r.igv, r.tot_periodo,
    r.electrif_rural, r.comp_distribucion, r.comp_generadora, r.comp_interrup, r.comp_calidad,
    r.comp_frec_ant, r.comp_frec_des, r.comp_serv, r.comp_norma_tec, r.comp_deuda_ant, r.comp_deuda_meses,
    r.comp_dev_reclamo, r.comp_nota_deb_cred, r.comp_aporte_reemb, r.comp_otros_inafectos,
    r.comp_redondeo_act_pos, r.comp_redondeo_act_neg,
    r.costo_medio, r.total_a_pagar, r.valor_venta, r.deuda_anterior, r.devolucion, r.fecha_liquidacion,
    clock_timestamp()::timestamp(0), 'raw.web_mm_indra_energia'
  FROM cand_dups r
  LEFT JOIN public.error_energia e
         ON e.num_recibo   = r.num_recibo
        AND ( (e.per_consumo IS NULL AND r.per_consumo IS NULL) OR e.per_consumo = r.per_consumo )
        AND e.tabla_origen = 'raw.web_mm_indra_energia'
        AND e.fecha_carga::date = clock_timestamp()::date
  WHERE e.num_recibo IS NULL;

  GET DIAGNOSTICS v_ins_err_dup = ROW_COUNT;

  WITH claves_dup AS (
    SELECT num_recibo, per_consumo
    FROM raw.web_mm_indra_energia
    GROUP BY num_recibo, per_consumo
    HAVING COUNT(*) > 1
  ),
  rid_dup AS (
    SELECT r.ctid AS rid
    FROM raw.web_mm_indra_energia r
    JOIN claves_dup d
      ON d.num_recibo = r.num_recibo
     AND ( (d.per_consumo IS NULL AND r.per_consumo IS NULL) OR d.per_consumo = r.per_consumo )
  )
  DELETE FROM raw.web_mm_indra_energia r
  USING rid_dup x
  WHERE r.ctid = x.rid;

  GET DIAGNOSTICS v_del_dup_raw = ROW_COUNT;

  /* ===================== 2) TRANSFORMACIÓN + CARGA A ODS ===================== */
  WITH base AS (
    SELECT
      r.ctid AS rid,

      -- num_recibo limpio
      CASE
        WHEN r.num_recibo IS NULL OR btrim(r.num_recibo) = '' OR lower(btrim(r.num_recibo)) IN ('nan','null')
          THEN NULL
        ELSE left(btrim(r.num_recibo), 250)
      END AS num_recibo,

      -- cod_suministro como TEXTO (varchar 250)
      NULLIF(NULLIF(btrim(r.cod_suministro), 'NaN'), '')::varchar(250) AS cod_suministro,

      NULLIF(NULLIF(btrim(r.tarifa), 'NaN'), '')::varchar(10)  AS tarifa,

      /* ==== pot_contr ahora VARCHAR(255) ==== */
      NULLIF(NULLIF(btrim(r.pot_contr), 'NaN'), '')::varchar(255) AS pot_contr,
      -- Variante con limpieza de símbolos (opcional):
      -- CASE
      --   WHEN r.pot_contr IS NULL OR btrim(r.pot_contr) = '' OR lower(btrim(r.pot_contr)) IN ('nan','null')
      --     THEN NULL
      --   ELSE left(regexp_replace(btrim(r.pot_contr), '[^\d\.\,\-\w ]', '', 'g'), 255)
      -- END::varchar(255) AS pot_contr,

      NULLIF(NULLIF(btrim(r.titularidad),  'NaN'), '')::varchar(255) AS titularidad,
      NULLIF(NULLIF(btrim(r.distribuidor), 'NaN'), '')::varchar(255) AS distribuidor,

      /* ==== FECHAS robustas ==== */
      -- fec_emision
      CASE
        WHEN r.fec_emision IS NULL OR btrim(r.fec_emision) = '' OR lower(btrim(r.fec_emision)) IN ('nan','null') THEN NULL
        ELSE
          CASE
            WHEN regexp_replace(split_part(r.fec_emision,' ',1), '[\.\-]', '/', 'g') ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
              THEN to_date(regexp_replace(split_part(r.fec_emision,' ',1), '[\.\-]', '/', 'g'), 'DD/MM/YYYY')
            WHEN regexp_replace(split_part(r.fec_emision,' ',1), '[\.\-]', '/', 'g') ~ '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
              THEN to_date(regexp_replace(split_part(r.fec_emision,' ',1), '[\.\-]', '/', 'g'), 'YYYY/MM/DD')
            ELSE NULL
          END
      END AS fec_emision,

      -- fec_vencimiento
      CASE
        WHEN r.fec_vencimiento IS NULL OR btrim(r.fec_vencimiento) = '' OR lower(btrim(r.fec_vencimiento)) IN ('nan','null') THEN NULL
        ELSE
          CASE
            WHEN regexp_replace(split_part(r.fec_vencimiento,' ',1), '[\.\-]', '/', 'g') ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
              THEN to_date(regexp_replace(split_part(r.fec_vencimiento,' ',1), '[\.\-]', '/', 'g'), 'DD/MM/YYYY')
            WHEN regexp_replace(split_part(r.fec_vencimiento,' ',1), '[\.\-]', '/', 'g') ~ '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
              THEN to_date(regexp_replace(split_part(r.fec_vencimiento,' ',1), '[\.\-]', '/', 'g'), 'YYYY/MM/DD')
            ELSE NULL
          END
      END AS fec_vencimiento,

      -- fec_lect_actual
      CASE
        WHEN r.fec_lect_actual IS NULL OR btrim(r.fec_lect_actual) = '' OR lower(btrim(r.fec_lect_actual)) IN ('nan','null') THEN NULL
        ELSE
          CASE
            WHEN regexp_replace(split_part(r.fec_lect_actual,' ',1), '[\.\-]', '/', 'g') ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
              THEN to_date(regexp_replace(split_part(r.fec_lect_actual,' ',1), '[\.\-]', '/', 'g'), 'DD/MM/YYYY')
            WHEN regexp_replace(split_part(r.fec_lect_actual,' ',1), '[\.\-]', '/', 'g') ~ '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
              THEN to_date(regexp_replace(split_part(r.fec_lect_actual,' ',1), '[\.\-]', '/', 'g'), 'YYYY/MM/DD')
            ELSE NULL
          END
      END AS fec_lect_actual,

      -- fec_lect_anterior
      CASE
        WHEN r.fec_lect_anterior IS NULL OR btrim(r.fec_lect_anterior) = '' OR lower(btrim(r.fec_lect_anterior)) IN ('nan','null') THEN NULL
        ELSE
          CASE
            WHEN regexp_replace(split_part(r.fec_lect_anterior,' ',1), '[\.\-]', '/', 'g') ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
              THEN to_date(regexp_replace(split_part(r.fec_lect_anterior,' ',1), '[\.\-]', '/', 'g'), 'DD/MM/YYYY')
            WHEN regexp_replace(split_part(r.fec_lect_anterior,' ',1), '[\.\-]', '/', 'g') ~ '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
              THEN to_date(regexp_replace(split_part(r.fec_lect_anterior,' ',1), '[\.\-]', '/', 'g'), 'YYYY/MM/DD')
            ELSE NULL
          END
      END AS fec_lect_anterior,

      NULLIF(NULLIF(btrim(r.per_consumo), 'NaN'), '')::varchar(50) AS per_consumo,

      public.try_numeric(r.ea_lec_act_hp)::numeric(18,2)  AS ea_lec_act_hp,
      public.try_numeric(r.ea_lec_act_fp)::numeric(18,2)  AS ea_lec_act_fp,
      public.try_numeric(r.ea_lec_act_tot)::numeric(18,2) AS ea_lec_act_tot,
      public.try_numeric(r.ea_lec_ant_hp)::numeric(18,2)  AS ea_lec_ant_hp,
      public.try_numeric(r.ea_lec_ant_fp)::numeric(18,2)  AS ea_lec_ant_fp,
      public.try_numeric(r.ea_lec_ant_tot)::numeric(18,2) AS ea_lec_ant_tot,

      public.try_numeric(r.ea_consumo_hp)::numeric(18,2)  AS ea_consumo_hp,
      public.try_numeric(r.ea_consumo_fp)::numeric(18,2)  AS ea_consumo_fp,
      public.try_numeric(r.ea_consumo_tot)::numeric(18,2) AS ea_consumo_tot,

      public.try_numeric(r.ea_pu_hp)::numeric(18,6)       AS ea_pu_hp,
      public.try_numeric(r.ea_pu_fp)::numeric(18,6)       AS ea_pu_fp,
      public.try_numeric(r.ea_prec_unit_tot)::numeric(18,6) AS ea_prec_unit_tot,

      public.try_numeric(r.ea_importe_hp)::numeric(18,2)  AS ea_importe_hp,
      public.try_numeric(r.ea_importe_fp)::numeric(18,2)  AS ea_importe_fp,
      public.try_numeric(r.ea_importe_tot)::numeric(18,2) AS ea_importe_tot,

      public.try_numeric(r.pot_lect_act_hp)::numeric(18,2)   AS pot_lect_act_hp,
      public.try_numeric(r.pot_lect_act_fp)::numeric(18,2)   AS pot_lect_act_fp,
      public.try_numeric(r.pot_lect_ant_hp)::numeric(18,2)   AS pot_lect_ant_hp,
      public.try_numeric(r.pot_lect_ant_fp)::numeric(18,2)   AS pot_lect_ant_fp,
      public.try_numeric(r.pot_reg_hp)::numeric(18,2)        AS pot_reg_hp,
      public.try_numeric(r.pot_reg_fp)::numeric(18,2)        AS pot_reg_fp,
      public.try_numeric(r.pot_gen_fact)::numeric(18,2)      AS pot_gen_fact,
      public.try_numeric(r.pot_gen_pu)::numeric(18,6)        AS pot_gen_pu,
      public.try_numeric(r.pot_gen_importe)::numeric(18,2)   AS pot_gen_importe,
      public.try_numeric(r.pot_dist_fact)::numeric(18,2)     AS pot_dist_fact,
      public.try_numeric(r.pot_dist_pu)::numeric(18,6)       AS pot_dist_pu,
      public.try_numeric(r.pot_dist_importe)::numeric(18,2)  AS pot_dist_importe,
      public.try_numeric(r.pot_bt6_pu)::numeric(18,6)        AS pot_bt6_pu,
      public.try_numeric(r.pot_bt6_monto)::numeric(18,2)     AS pot_bt6_monto,

      public.try_numeric(r.er_lec_act)::numeric(18,2)     AS er_lec_act,
      public.try_numeric(r.er_lec_ant)::numeric(18,2)     AS er_lec_ant,
      public.try_numeric(r.er_cons)::numeric(18,2)        AS er_cons,
      public.try_numeric(r.er_fact)::numeric(18,2)        AS er_fact,
      public.try_numeric(r.er_pu)::numeric(18,6)          AS er_pu,
      public.try_numeric(r.er_importe)::numeric(18,2)     AS er_importe,

      NULLIF(NULLIF(btrim(r.calif_cliente), 'NaN'), '')::varchar(100) AS calif_cliente,
      trunc(public.try_numeric(r.dias_punta))::int        AS dias_punta,
      trunc(public.try_numeric(r.horas_punta))::int       AS horas_punta,
      NULLIF(NULLIF(btrim(r.factor_calif), 'NaN'), '')::varchar(50) AS factor_calif,

      public.try_numeric(r.cargo_fijo)::numeric(18,2)        AS cargo_fijo,
      public.try_numeric(r.mantenimiento)::numeric(18,2)     AS mantenimiento,
      public.try_numeric(r.alumbrado)::numeric(18,2)         AS alumbrado,
      public.try_numeric(r.recup_energia)::numeric(18,2)     AS recup_energia,
      public.try_numeric(r.mora)::numeric(18,2)              AS mora,
      public.try_numeric(r.reconexion)::numeric(18,2)        AS reconexion,
      public.try_numeric(r.ajuste_tarifa)::numeric(18,2)     AS ajuste_tarifa,
      public.try_numeric(r.dist_factura)::numeric(18,2)      AS dist_factura,
      public.try_numeric(r.ajuste_alumb)::numeric(18,2)      AS ajuste_alumb,
      public.try_numeric(r.otros_afectos)::numeric(18,2)     AS otros_afectos,
      public.try_numeric(r.base_imponible)::numeric(18,2)    AS base_imponible,
      public.try_numeric(r.igv)::numeric(18,2)               AS igv,
      public.try_numeric(r.tot_periodo)::numeric(18,2)       AS tot_periodo,

      public.try_numeric(r.electrif_rural)::numeric(18,2)    AS electrif_rural,
      public.try_numeric(r.comp_distribucion)::numeric(18,2) AS comp_distribucion,
      public.try_numeric(r.comp_generadora)::numeric(18,2)   AS comp_generadora,
      public.try_numeric(r.comp_interrup)::numeric(18,2)     AS comp_interrup,
      public.try_numeric(r.comp_calidad)::numeric(18,2)      AS comp_calidad,
      public.try_numeric(r.comp_frec_ant)::numeric(18,2)     AS comp_frec_ant,
      public.try_numeric(r.comp_frec_des)::numeric(18,2)     AS comp_frec_des,
      public.try_numeric(r.comp_serv)::numeric(18,2)         AS comp_serv,
      public.try_numeric(r.comp_norma_tec)::numeric(18,2)    AS comp_norma_tec,
      public.try_numeric(r.comp_deuda_ant)::numeric(18,2)    AS comp_deuda_ant,
      trunc(public.try_numeric(r.comp_deuda_meses))::int     AS comp_deuda_meses,

      public.try_numeric(r.comp_dev_reclamo)::numeric(18,2)  AS comp_dev_reclamo,
      public.try_numeric(r.comp_nota_deb_cred)::numeric(18,2) AS comp_nota_deb_cred,
      public.try_numeric(r.comp_aporte_reemb)::numeric(18,2) AS comp_aporte_reemb,
      public.try_numeric(r.comp_otros_inafectos)::numeric(18,2) AS comp_otros_inafectos,
      public.try_numeric(r.comp_redondeo_act_pos)::numeric(18,2) AS comp_redondeo_act_pos,
      public.try_numeric(r.comp_redondeo_act_neg)::numeric(18,2) AS comp_redondeo_act_neg,

      public.try_numeric(r.costo_medio)::numeric(18,2)       AS costo_medio,
      public.try_numeric(r.total_a_pagar)::numeric(18,2)     AS total_a_pagar,
      public.try_numeric(r.valor_venta)::numeric(18,2)       AS valor_venta,
      public.try_numeric(r.deuda_anterior)::numeric(18,2)    AS deuda_anterior,
      public.try_numeric(r.devolucion)::numeric(18,2)        AS devolucion,

      -- fecha_liquidacion
      CASE
        WHEN r.fecha_liquidacion IS NULL OR btrim(r.fecha_liquidacion) = '' OR lower(btrim(r.fecha_liquidacion)) IN ('nan','null') THEN NULL
        ELSE
          CASE
            WHEN regexp_replace(split_part(r.fecha_liquidacion,' ',1), '[\.\-]', '/', 'g') ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
              THEN to_date(regexp_replace(split_part(r.fecha_liquidacion,' ',1), '[\.\-]', '/', 'g'), 'DD/MM/YYYY')
            WHEN regexp_replace(split_part(r.fecha_liquidacion,' ',1), '[\.\-]', '/', 'g') ~ '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
              THEN to_date(regexp_replace(split_part(r.fecha_liquidacion,' ',1), '[\.\-]', '/', 'g'), 'YYYY/MM/DD')
            ELSE NULL
          END
      END AS fecha_liquidacion
    FROM raw.web_mm_indra_energia r
  ),
  dedup AS (
    SELECT DISTINCT ON (num_recibo) *
    FROM base
    WHERE num_recibo IS NOT NULL
    ORDER BY num_recibo, rid
  )
  INSERT INTO ods.web_hm_indra_energia (
    num_recibo, cod_suministro, tarifa, pot_contr, titularidad, distribuidor,
    fec_emision, fec_vencimiento, fec_lect_actual, fec_lect_anterior, per_consumo,
    ea_lec_act_hp, ea_lec_act_fp, ea_lec_act_tot, ea_lec_ant_hp, ea_lec_ant_fp, ea_lec_ant_tot,
    ea_consumo_hp, ea_consumo_fp, ea_consumo_tot, ea_pu_hp, ea_pu_fp, ea_prec_unit_tot,
    ea_importe_hp, ea_importe_fp, ea_importe_tot,
    pot_lect_act_hp, pot_lect_act_fp, pot_lect_ant_hp, pot_lect_ant_fp, pot_reg_hp, pot_reg_fp,
    pot_gen_fact, pot_gen_pu, pot_gen_importe, pot_dist_fact, pot_dist_pu, pot_dist_importe,
    pot_bt6_pu, pot_bt6_monto,
    er_lec_act, er_lec_ant, er_cons, er_fact, er_pu, er_importe,
    calif_cliente, dias_punta, horas_punta, factor_calif,
    cargo_fijo, mantenimiento, alumbrado, recup_energia, mora, reconexion, ajuste_tarifa,
    dist_factura, ajuste_alumb, otros_afectos, base_imponible, igv, tot_periodo,
    electrif_rural, comp_distribucion, comp_generadora, comp_interrup, comp_calidad,
    comp_frec_ant, comp_frec_des, comp_serv, comp_norma_tec, comp_deuda_ant, comp_deuda_meses,
    comp_dev_reclamo, comp_nota_deb_cred, comp_aporte_reemb, comp_otros_inafectos,
    comp_redondeo_act_pos, comp_redondeo_act_neg,
    costo_medio, total_a_pagar, valor_venta, deuda_anterior, devolucion, fecha_liquidacion
  )
  SELECT
    d.num_recibo, d.cod_suministro, d.tarifa, d.pot_contr, d.titularidad, d.distribuidor,
    d.fec_emision, d.fec_vencimiento, d.fec_lect_actual, d.fec_lect_anterior, d.per_consumo,
    d.ea_lec_act_hp, d.ea_lec_act_fp, d.ea_lec_act_tot, d.ea_lec_ant_hp, d.ea_lec_ant_fp, d.ea_lec_ant_tot,
    d.ea_consumo_hp, d.ea_consumo_fp, d.ea_consumo_tot, d.ea_pu_hp, d.ea_pu_fp, d.ea_prec_unit_tot,
    d.ea_importe_hp, d.ea_importe_fp, d.ea_importe_tot,
    d.pot_lect_act_hp, d.pot_lect_act_fp, d.pot_lect_ant_hp, d.pot_lect_ant_fp, d.pot_reg_hp, d.pot_reg_fp,
    d.pot_gen_fact, d.pot_gen_pu, d.pot_gen_importe, d.pot_dist_fact, d.pot_dist_pu, d.pot_dist_importe,
    d.pot_bt6_pu, d.pot_bt6_monto,
    d.er_lec_act, d.er_lec_ant, d.er_cons, d.er_fact, d.er_pu, d.er_importe,
    d.calif_cliente, d.dias_punta, d.horas_punta, d.factor_calif,
    d.cargo_fijo, d.mantenimiento, d.alumbrado, d.recup_energia, d.mora, d.reconexion, d.ajuste_tarifa,
    d.dist_factura, d.ajuste_alumb, d.otros_afectos, d.base_imponible, d.igv, d.tot_periodo,
    d.electrif_rural, d.comp_distribucion, d.comp_generadora, d.comp_interrup, d.comp_calidad,
    d.comp_frec_ant, d.comp_frec_des, d.comp_serv, d.comp_norma_tec, d.comp_deuda_ant, d.comp_deuda_meses,
    d.comp_dev_reclamo, d.comp_nota_deb_cred, d.comp_aporte_reemb, d.comp_otros_inafectos,
    d.comp_redondeo_act_pos, d.comp_redondeo_act_neg,
    d.costo_medio, d.total_a_pagar, d.valor_venta, d.deuda_anterior, d.devolucion, d.fecha_liquidacion
  FROM dedup d
  LEFT JOIN ods.web_hm_indra_energia o
    ON o.num_recibo = d.num_recibo
  WHERE o.num_recibo IS NULL;

  GET DIAGNOSTICS v_inserted_ods = ROW_COUNT;

  /* ===================== LOG ===================== */
  v_estado := 'DONE';
  v_msj := format(
    'Duplicados movidos a error_energia=%s | Duplicados borrados RAW=%s | Insertados en ODS=%s | Origen=raw.web_mm_indra_energia',
    v_ins_err_dup, v_del_dup_raw, v_inserted_ods
  );

  CALL public.sp_grabar_log_sp(
    p_id_sp      => v_id_sp,
    p_inicio     => v_inicio,
    p_fin        => clock_timestamp()::timestamp(0),
    p_inserted   => v_inserted_ods,
    p_updated    => NULL::integer,
    p_deleted    => v_del_dup_raw,
    p_nulls      => NULL::integer,
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
