
CREATE OR REPLACE PROCEDURE ods.sp_cargar_sftp_hm_clientes_libres()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio    timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp     integer      := 'ods.sp_cargar_sftp_hm_clientes_libres()'::regprocedure::oid::int;
  v_sp_name   text         := 'ods.sp_cargar_sftp_hm_clientes_libres()'::regprocedure::text;

  v_inserted  integer := 0;
  v_updated   integer := 0;
  v_deleted   integer := 0;
  v_nulls     integer := NULL;

  v_estado    varchar(50);
  v_msj       text;
BEGIN
  /*  UPSERT desde RAW -> ODS
      - stg: CTE con casteos/limpiezas desde RAW (todo venía como texto).
      - upd: UPDATE sólo si hay diferencias reales (IS DISTINCT FROM), RETURNING 1.
      - ins: INSERT anti-join por (num_recibo, per_consumo), RETURNING 1.
  */
  WITH
  stg AS (
    SELECT
      NULLIF(trim(r.num_recibo),'')::bigint                                   AS num_recibo,
      CASE WHEN NULLIF(trim(r.cod_suministro),'') IS NULL
           THEN NULL ELSE NULLIF(trim(r.cod_suministro),'')::int END          AS cod_suministro,
      LEFT(NULLIF(trim(r.tarifa),''),10)                                      AS tarifa,

      -- pot_contr como texto (varchar 255)
      LEFT(NULLIF(trim(r.pot_contr),''),255)::varchar(255)                    AS pot_contr,

      NULLIF(trim(r.titularidad),'')                                          AS titularidad,
      NULLIF(trim(r.distribuidor),'')                                         AS distribuidor,
      CASE
        WHEN NULLIF(trim(r.fec_emision),'') IS NULL THEN NULL
        WHEN r.fec_emision ~ '^\d{4}-\d{2}-\d{2}$' THEN r.fec_emision::date
        WHEN r.fec_emision ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(r.fec_emision,'DD/MM/YYYY')
        WHEN r.fec_emision ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(r.fec_emision,'DD-MM-YYYY')
        ELSE NULL
      END                                                                       AS fec_emision,
      CASE
        WHEN NULLIF(trim(r.fec_vencimiento),'') IS NULL THEN NULL
        WHEN r.fec_vencimiento ~ '^\d{4}-\d{2}-\d{2}$' THEN r.fec_vencimiento::date
        WHEN r.fec_vencimiento ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(r.fec_vencimiento,'DD/MM/YYYY')
        WHEN r.fec_vencimiento ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(r.fec_vencimiento,'DD-MM-YYYY')
        ELSE NULL
      END                                                                       AS fec_vencimiento,
      CASE
        WHEN NULLIF(trim(r.fec_lect_actual),'') IS NULL THEN NULL
        WHEN r.fec_lect_actual ~ '^\d{4}-\d{2}-\d{2}$' THEN r.fec_lect_actual::date
        WHEN r.fec_lect_actual ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(r.fec_lect_actual,'DD/MM/YYYY')
        WHEN r.fec_lect_actual ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(r.fec_lect_actual,'DD-MM-YYYY')
        ELSE NULL
      END                                                                       AS fec_lect_actual,
      CASE
        WHEN NULLIF(trim(r.fec_lect_anterior),'') IS NULL THEN NULL
        WHEN r.fec_lect_anterior ~ '^\d{4}-\d{2}-\d{2}$' THEN r.fec_lect_anterior::date
        WHEN r.fec_lect_anterior ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(r.fec_lect_anterior,'DD/MM/YYYY')
        WHEN r.fec_lect_anterior ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(r.fec_lect_anterior,'DD-MM-YYYY')
        ELSE NULL
      END                                                                       AS fec_lect_anterior,
      LEFT(NULLIF(trim(r.per_consumo),''),50)                                   AS per_consumo,

      NULLIF(replace(trim(r.ea_lec_act_hp),',','.'),'')::numeric(18,2)         AS ea_lec_act_hp,
      NULLIF(replace(trim(r.ea_lec_act_fp),',','.'),'')::numeric(18,2)         AS ea_lec_act_fp,
      NULLIF(replace(trim(r.ea_lec_act_tot),',','.'),'')::numeric(18,2)        AS ea_lec_act_tot,
      NULLIF(replace(trim(r.ea_lec_ant_hp),',','.'),'')::numeric(18,2)         AS ea_lec_ant_hp,
      NULLIF(replace(trim(r.ea_lec_ant_fp),',','.'),'')::numeric(18,2)         AS ea_lec_ant_fp,
      NULLIF(replace(trim(r.ea_lec_ant_tot),',','.'),'')::numeric(18,2)        AS ea_lec_ant_tot,
      NULLIF(replace(trim(r.ea_consumo_hp),',','.'),'')::numeric(18,2)         AS ea_consumo_hp,
      NULLIF(replace(trim(r.ea_consumo_fp),',','.'),'')::numeric(18,2)         AS ea_consumo_fp,
      NULLIF(replace(trim(r.ea_consumo_tot),',','.'),'')::numeric(18,2)        AS ea_consumo_tot,
      NULLIF(replace(trim(r.ea_pu_hp),',','.'),'')::numeric(18,6)              AS ea_pu_hp,
      NULLIF(replace(trim(r.ea_pu_fp),',','.'),'')::numeric(18,6)              AS ea_pu_fp,
      NULLIF(replace(trim(r.ea_prec_unit_tot),',','.'),'')::numeric(18,6)      AS ea_prec_unit_tot,
      NULLIF(replace(trim(r.ea_importe_hp),',','.'),'')::numeric(18,2)         AS ea_importe_hp,
      NULLIF(replace(trim(r.ea_importe_fp),',','.'),'')::numeric(18,2)         AS ea_importe_fp,
      NULLIF(replace(trim(r.ea_importe_tot),',','.'),'')::numeric(18,2)        AS ea_importe_tot,

      NULLIF(replace(trim(r.pot_lect_act_hp),',','.'),'')::numeric(18,2)       AS pot_lect_act_hp,
      NULLIF(replace(trim(r.pot_lect_act_fp),',','.'),'')::numeric(18,2)       AS pot_lect_act_fp,
      NULLIF(replace(trim(r.pot_lect_ant_hp),',','.'),'')::numeric(18,2)       AS pot_lect_ant_hp,
      NULLIF(replace(trim(r.pot_lect_ant_fp),',','.'),'')::numeric(18,2)       AS pot_lect_ant_fp,
      NULLIF(replace(trim(r.pot_reg_hp),',','.'),'')::numeric(18,2)            AS pot_reg_hp,
      NULLIF(replace(trim(r.pot_reg_fp),',','.'),'')::numeric(18,2)            AS pot_reg_fp,
      NULLIF(replace(trim(r.pot_gen_fact),',','.'),'')::numeric(18,2)          AS pot_gen_fact,
      NULLIF(replace(trim(r.pot_gen_pu),',','.'),'')::numeric(18,6)            AS pot_gen_pu,
      NULLIF(replace(trim(r.pot_gen_importe),',','.'),'')::numeric(18,2)       AS pot_gen_importe,
      NULLIF(replace(trim(r.pot_dist_fact),',','.'),'')::numeric(18,2)         AS pot_dist_fact,
      NULLIF(replace(trim(r.pot_dist_pu),',','.'),'')::numeric(18,6)           AS pot_dist_pu,
      NULLIF(replace(trim(r.pot_dist_importe),',','.'),'')::numeric(18,2)      AS pot_dist_importe,
      NULLIF(replace(trim(r.pot_bt6_pu),',','.'),'')::numeric(18,6)            AS pot_bt6_pu,
      NULLIF(replace(trim(r.pot_bt6_monto),',','.'),'')::numeric(18,2)         AS pot_bt6_monto,

      NULLIF(replace(trim(r.er_lec_act),',','.'),'')::numeric(18,2)            AS er_lec_act,
      NULLIF(replace(trim(r.er_lec_ant),',','.'),'')::numeric(18,2)            AS er_lec_ant,
      NULLIF(replace(trim(r.er_cons),',','.'),'')::numeric(18,2)               AS er_cons,
      NULLIF(replace(trim(r.er_fact),',','.'),'')::numeric(18,2)               AS er_fact,
      NULLIF(replace(trim(r.er_pu),',','.'),'')::numeric(18,6)                 AS er_pu,
      NULLIF(replace(trim(r.er_importe),',','.'),'')::numeric(18,2)            AS er_importe,

      LEFT(NULLIF(trim(r.calif_cliente),''),100)                               AS calif_cliente,
      CASE WHEN NULLIF(trim(r.dias_punta),'') IS NULL THEN NULL
           ELSE NULLIF(trim(r.dias_punta),'')::int END                         AS dias_punta,
      CASE WHEN NULLIF(trim(r.horas_punta),'') IS NULL THEN NULL
           ELSE NULLIF(trim(r.horas_punta),'')::int END                        AS horas_punta,
      LEFT(NULLIF(trim(r.factor_calif),''),50)                                 AS factor_calif,

      NULLIF(replace(trim(r.cargo_fijo),',','.'),'')::numeric(18,2)            AS cargo_fijo,
      NULLIF(replace(trim(r.mantenimiento),',','.'),'')::numeric(18,2)         AS mantenimiento,
      NULLIF(replace(trim(r.alumbrado),',','.'),'')::numeric(18,2)             AS alumbrado,
      NULLIF(replace(trim(r.recup_energia),',','.'),'')::numeric(18,2)         AS recup_energia,
      NULLIF(replace(trim(r.mora),',','.'),'')::numeric(18,2)                  AS mora,
      NULLIF(replace(trim(r.reconexion),',','.'),'')::numeric(18,2)            AS reconexion,
      NULLIF(replace(trim(r.ajuste_tarifa),',','.'),'')::numeric(18,2)         AS ajuste_tarifa,
      NULLIF(replace(trim(r.dist_factura),',','.'),'')::numeric(18,2)          AS dist_factura,
      NULLIF(replace(trim(r.ajuste_alumb),',','.'),'')::numeric(18,2)          AS ajuste_alumb,
      NULLIF(replace(trim(r.otros_afectos),',','.'),'')::numeric(18,2)         AS otros_afectos,
      NULLIF(replace(trim(r.base_imponible),',','.'),'')::numeric(18,2)        AS base_imponible,
      NULLIF(replace(trim(r.igv),',','.'),'')::numeric(18,2)                   AS igv,
      NULLIF(replace(trim(r.tot_periodo),',','.'),'')::numeric(18,2)           AS tot_periodo,
      NULLIF(replace(trim(r.electrif_rural),',','.'),'')::numeric(18,2)        AS electrif_rural,

      NULLIF(replace(trim(r.comp_distribucion),',','.'),'')::numeric(18,2)     AS comp_distribucion,
      NULLIF(replace(trim(r.comp_generadora),',','.'),'')::numeric(18,2)       AS comp_generadora,
      NULLIF(replace(trim(r.comp_interrup),',','.'),'')::numeric(18,2)         AS comp_interrup,
      NULLIF(replace(trim(r.comp_calidad),',','.'),'')::numeric(18,2)          AS comp_calidad,
      NULLIF(replace(trim(r.comp_frec_ant),',','.'),'')::numeric(18,2)         AS comp_frec_ant,
      NULLIF(replace(trim(r.comp_frec_des),',','.'),'')::numeric(18,2)         AS comp_frec_des,
      NULLIF(replace(trim(r.comp_serv),',','.'),'')::numeric(18,2)             AS comp_serv,
      NULLIF(replace(trim(r.comp_norma_tec),',','.'),'')::numeric(18,2)        AS comp_norma_tec,
      NULLIF(replace(trim(r.comp_deuda_ant),',','.'),'')::numeric(18,2)        AS comp_deuda_ant,
      CASE WHEN NULLIF(trim(r.comp_deuda_meses),'') IS NULL THEN NULL
           ELSE NULLIF(trim(r.comp_deuda_meses),'')::int END                   AS comp_deuda_meses,
      NULLIF(replace(trim(r.comp_dev_reclamo),',','.'),'')::numeric(18,2)      AS comp_dev_reclamo,
      NULLIF(replace(trim(r.comp_nota_deb_cred),',','.'),'')::numeric(18,2)    AS comp_nota_deb_cred,
      NULLIF(replace(trim(r.comp_aporte_reemb),',','.'),'')::numeric(18,2)     AS comp_aporte_reemb,
      NULLIF(replace(trim(r.comp_otros_inafectos),',','.'),'')::numeric(18,2)  AS comp_otros_inafectos,
      NULLIF(replace(trim(r.comp_redondeo_act_pos),',','.'),'')::numeric(18,2) AS comp_redondeo_act_pos,
      NULLIF(replace(trim(r.comp_redondeo_act_neg),',','.'),'')::numeric(18,2) AS comp_redondeo_act_neg,

      NULLIF(replace(trim(r.costo_medio),',','.'),'')::numeric(18,2)           AS costo_medio,
      NULLIF(replace(trim(r.total_a_pagar),',','.'),'')::numeric(18,2)         AS total_a_pagar,
      NULLIF(replace(trim(r.valor_venta),',','.'),'')::numeric(18,2)           AS valor_venta,
      NULLIF(replace(trim(r.deuda_anterior),',','.'),'')::numeric(18,2)        AS deuda_anterior,
      NULLIF(replace(trim(r.devolucion),',','.'),'')::numeric(18,2)            AS devolucion,

      CASE
        WHEN NULLIF(trim(r.fecha_liquidacion),'') IS NULL THEN NULL
        WHEN r.fecha_liquidacion ~ '^\d{4}-\d{2}-\d{2}$' THEN r.fecha_liquidacion::date
        WHEN r.fecha_liquidacion ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(r.fecha_liquidacion,'DD/MM/YYYY')
        WHEN r.fecha_liquidacion ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(r.fecha_liquidacion,'DD-MM-YYYY')
        ELSE NULL
      END                                                                       AS fecha_liquidacion
    FROM raw.sftp_mm_clientes_libres r
    WHERE NULLIF(trim(r.num_recibo),'') IS NOT NULL
      AND NULLIF(trim(r.per_consumo),'') IS NOT NULL
  ),
  upd AS (
    UPDATE ods.sftp_hm_clientes_libres t
    SET
      cod_suministro      = s.cod_suministro,
      tarifa              = s.tarifa,
      pot_contr           = s.pot_contr,
      titularidad         = s.titularidad,
      distribuidor        = s.distribuidor,
      fec_emision         = s.fec_emision,
      fec_vencimiento     = s.fec_vencimiento,
      fec_lect_actual     = s.fec_lect_actual,
      fec_lect_anterior   = s.fec_lect_anterior,
      per_consumo         = s.per_consumo,
      ea_lec_act_hp       = s.ea_lec_act_hp,
      ea_lec_act_fp       = s.ea_lec_act_fp,
      ea_lec_act_tot      = s.ea_lec_act_tot,
      ea_lec_ant_hp       = s.ea_lec_ant_hp,
      ea_lec_ant_fp       = s.ea_lec_ant_fp,
      ea_lec_ant_tot      = s.ea_lec_ant_tot,
      ea_consumo_hp       = s.ea_consumo_hp,
      ea_consumo_fp       = s.ea_consumo_fp,
      ea_consumo_tot      = s.ea_consumo_tot,
      ea_pu_hp            = s.ea_pu_hp,
      ea_pu_fp            = s.ea_pu_fp,
      ea_prec_unit_tot    = s.ea_prec_unit_tot,
      ea_importe_hp       = s.ea_importe_hp,
      ea_importe_fp       = s.ea_importe_fp,
      ea_importe_tot      = s.ea_importe_tot,
      pot_lect_act_hp     = s.pot_lect_act_hp,
      pot_lect_act_fp     = s.pot_lect_act_fp,
      pot_lect_ant_hp     = s.pot_lect_ant_hp,
      pot_lect_ant_fp     = s.pot_lect_ant_fp,
      pot_reg_hp          = s.pot_reg_hp,
      pot_reg_fp          = s.pot_reg_fp,
      pot_gen_fact        = s.pot_gen_fact,
      pot_gen_pu          = s.pot_gen_pu,
      pot_gen_importe     = s.pot_gen_importe,
      pot_dist_fact       = s.pot_dist_fact,
      pot_dist_pu         = s.pot_dist_pu,
      pot_dist_importe    = s.pot_dist_importe,
      pot_bt6_pu          = s.pot_bt6_pu,
      pot_bt6_monto       = s.pot_bt6_monto,
      er_lec_act          = s.er_lec_act,
      er_lec_ant          = s.er_lec_ant,
      er_cons             = s.er_cons,
      er_fact             = s.er_fact,
      er_pu               = s.er_pu,
      er_importe          = s.er_importe,
      calif_cliente       = s.calif_cliente,
      dias_punta          = s.dias_punta,
      horas_punta         = s.horas_punta,
      factor_calif        = s.factor_calif,
      cargo_fijo          = s.cargo_fijo,
      mantenimiento       = s.mantenimiento,
      alumbrado           = s.alumbrado,
      recup_energia       = s.recup_energia,
      mora                = s.mora,
      reconexion          = s.reconexion,
      ajuste_tarifa       = s.ajuste_tarifa,
      dist_factura        = s.dist_factura,
      ajuste_alumb        = s.ajuste_alumb,
      otros_afectos       = s.otros_afectos,
      base_imponible      = s.base_imponible,
      igv                 = s.igv,
      tot_periodo         = s.tot_periodo,
      electrif_rural      = s.electrif_rural,
      comp_distribucion   = s.comp_distribucion,
      comp_generadora     = s.comp_generadora,
      comp_interrup       = s.comp_interrup,
      comp_calidad        = s.comp_calidad,
      comp_frec_ant       = s.comp_frec_ant,
      comp_frec_des       = s.comp_frec_des,
      comp_serv           = s.comp_serv,
      comp_norma_tec      = s.comp_norma_tec,
      comp_deuda_ant      = s.comp_deuda_ant,
      comp_deuda_meses    = s.comp_deuda_meses,
      comp_dev_reclamo    = s.comp_dev_reclamo,
      comp_nota_deb_cred  = s.comp_nota_deb_cred,
      comp_aporte_reemb   = s.comp_aporte_reemb,
      comp_otros_inafectos= s.comp_otros_inafectos,
      comp_redondeo_act_pos = s.comp_redondeo_act_pos,
      comp_redondeo_act_neg = s.comp_redondeo_act_neg,
      costo_medio         = s.costo_medio,
      total_a_pagar       = s.total_a_pagar,
      valor_venta         = s.valor_venta,
      deuda_anterior      = s.deuda_anterior,
      devolucion          = s.devolucion,
      fecha_liquidacion   = s.fecha_liquidacion
    FROM stg s
    WHERE t.num_recibo = s.num_recibo
      AND t.per_consumo = s.per_consumo
      AND ROW(
        t.cod_suministro, t.tarifa, t.pot_contr, t.titularidad, t.distribuidor,
        t.fec_emision, t.fec_vencimiento, t.fec_lect_actual, t.fec_lect_anterior, t.per_consumo,
        t.ea_lec_act_hp, t.ea_lec_act_fp, t.ea_lec_act_tot, t.ea_lec_ant_hp, t.ea_lec_ant_fp, t.ea_lec_ant_tot,
        t.ea_consumo_hp, t.ea_consumo_fp, t.ea_consumo_tot, t.ea_pu_hp, t.ea_pu_fp, t.ea_prec_unit_tot,
        t.ea_importe_hp, t.ea_importe_fp, t.ea_importe_tot,
        t.pot_lect_act_hp, t.pot_lect_act_fp, t.pot_lect_ant_hp, t.pot_lect_ant_fp,
        t.pot_reg_hp, t.pot_reg_fp, t.pot_gen_fact, t.pot_gen_pu, t.pot_gen_importe,
        t.pot_dist_fact, t.pot_dist_pu, t.pot_dist_importe, t.pot_bt6_pu, t.pot_bt6_monto,
        t.er_lec_act, t.er_lec_ant, t.er_cons, t.er_fact, t.er_pu, t.er_importe,
        t.calif_cliente, t.dias_punta, t.horas_punta, t.factor_calif,
        t.cargo_fijo, t.mantenimiento, t.alumbrado, t.recup_energia, t.mora, t.reconexion,
        t.ajuste_tarifa, t.dist_factura, t.ajuste_alumb, t.otros_afectos, t.base_imponible, t.igv, t.tot_periodo,
        t.electrif_rural, t.comp_distribucion, t.comp_generadora, t.comp_interrup, t.comp_calidad,
        t.comp_frec_ant, t.comp_frec_des, t.comp_serv, t.comp_norma_tec, t.comp_deuda_ant, t.comp_deuda_meses,
        t.comp_dev_reclamo, t.comp_nota_deb_cred, t.comp_aporte_reemb, t.comp_otros_inafectos,
        t.comp_redondeo_act_pos, t.comp_redondeo_act_neg,
        t.costo_medio, t.total_a_pagar, t.valor_venta, t.deuda_anterior, t.devolucion,
        t.fecha_liquidacion
      ) IS DISTINCT FROM ROW(
        s.cod_suministro, s.tarifa, s.pot_contr, s.titularidad, s.distribuidor,
        s.fec_emision, s.fec_vencimiento, s.fec_lect_actual, s.fec_lect_anterior, s.per_consumo,
        s.ea_lec_act_hp, s.ea_lec_act_fp, s.ea_lec_act_tot, s.ea_lec_ant_hp, s.ea_lec_ant_fp, s.ea_lec_ant_tot,
        s.ea_consumo_hp, s.ea_consumo_fp, s.ea_consumo_tot, s.ea_pu_hp, s.ea_pu_fp, s.ea_prec_unit_tot,
        s.ea_importe_hp, s.ea_importe_fp, s.ea_importe_tot,
        s.pot_lect_act_hp, s.pot_lect_act_fp, s.pot_lect_ant_hp, s.pot_lect_ant_fp,
        s.pot_reg_hp, s.pot_reg_fp, s.pot_gen_fact, s.pot_gen_pu, s.pot_gen_importe,
        s.pot_dist_fact, s.pot_dist_pu, s.pot_dist_importe, s.pot_bt6_pu, s.pot_bt6_monto,
        s.er_lec_act, s.er_lec_ant, s.er_cons, s.er_fact, s.er_pu, s.er_importe,
        s.calif_cliente, s.dias_punta, s.horas_punta, s.factor_calif,
        s.cargo_fijo, s.mantenimiento, s.alumbrado, s.recup_energia, s.mora, s.reconexion,
        s.ajuste_tarifa, s.dist_factura, s.ajuste_alumb, s.otros_afectos, s.base_imponible, s.igv, s.tot_periodo,
        s.electrif_rural, s.comp_distribucion, s.comp_generadora, s.comp_interrup, s.comp_calidad,
        s.comp_frec_ant, s.comp_frec_des, s.comp_serv, s.comp_norma_tec, s.comp_deuda_ant, s.comp_deuda_meses,
        s.comp_dev_reclamo, s.comp_nota_deb_cred, s.comp_aporte_reemb, s.comp_otros_inafectos,
        s.comp_redondeo_act_pos, s.comp_redondeo_act_neg,
        s.costo_medio, s.total_a_pagar, s.valor_venta, s.deuda_anterior, s.devolucion,
        s.fecha_liquidacion
      )
    RETURNING 1
  ),
  ins AS (
    INSERT INTO ods.sftp_hm_clientes_libres (
      num_recibo, cod_suministro, tarifa, pot_contr, titularidad, distribuidor,
      fec_emision, fec_vencimiento, fec_lect_actual, fec_lect_anterior, per_consumo,
      ea_lec_act_hp, ea_lec_act_fp, ea_lec_act_tot, ea_lec_ant_hp, ea_lec_ant_fp, ea_lec_ant_tot,
      ea_consumo_hp, ea_consumo_fp, ea_consumo_tot, ea_pu_hp, ea_pu_fp, ea_prec_unit_tot,
      ea_importe_hp, ea_importe_fp, ea_importe_tot,
      pot_lect_act_hp, pot_lect_act_fp, pot_lect_ant_hp, pot_lect_ant_fp,
      pot_reg_hp, pot_reg_fp, pot_gen_fact, pot_gen_pu, pot_gen_importe,
      pot_dist_fact, pot_dist_pu, pot_dist_importe, pot_bt6_pu, pot_bt6_monto,
      er_lec_act, er_lec_ant, er_cons, er_fact, er_pu, er_importe,
      calif_cliente, dias_punta, horas_punta, factor_calif,
      cargo_fijo, mantenimiento, alumbrado, recup_energia, mora, reconexion,
      ajuste_tarifa, dist_factura, ajuste_alumb, otros_afectos, base_imponible, igv, tot_periodo,
      electrif_rural, comp_distribucion, comp_generadora, comp_interrup, comp_calidad,
      comp_frec_ant, comp_frec_des, comp_serv, comp_norma_tec, comp_deuda_ant, comp_deuda_meses,
      comp_dev_reclamo, comp_nota_deb_cred, comp_aporte_reemb, comp_otros_inafectos,
      comp_redondeo_act_pos, comp_redondeo_act_neg,
      costo_medio, total_a_pagar, valor_venta, deuda_anterior, devolucion, fecha_liquidacion
      -- OJO: no incluimos CREATION_*; se completan por DEFAULT de la tabla
    )
    SELECT
      s.num_recibo, s.cod_suministro, s.tarifa, s.pot_contr, s.titularidad, s.distribuidor,
      s.fec_emision, s.fec_vencimiento, s.fec_lect_actual, s.fec_lect_anterior, s.per_consumo,
      s.ea_lec_act_hp, s.ea_lec_act_fp, s.ea_lec_act_tot, s.ea_lec_ant_hp, s.ea_lec_ant_fp, s.ea_lec_ant_tot,
      s.ea_consumo_hp, s.ea_consumo_fp, s.ea_consumo_tot, s.ea_pu_hp, s.ea_pu_fp, s.ea_prec_unit_tot,
      s.ea_importe_hp, s.ea_importe_fp, s.ea_importe_tot,
      s.pot_lect_act_hp, s.pot_lect_act_fp, s.pot_lect_ant_hp, s.pot_lect_ant_fp,
      s.pot_reg_hp, s.pot_reg_fp, s.pot_gen_fact, s.pot_gen_pu, s.pot_gen_importe,
      s.pot_dist_fact, s.pot_dist_pu, s.pot_dist_importe, s.pot_bt6_pu, s.pot_bt6_monto,
      s.er_lec_act, s.er_lec_ant, s.er_cons, s.er_fact, s.er_pu, s.er_importe,
      s.calif_cliente, s.dias_punta, s.horas_punta, s.factor_calif,
      s.cargo_fijo, s.mantenimiento, s.alumbrado, s.recup_energia, s.mora, s.reconexion,
      s.ajuste_tarifa, s.dist_factura, s.ajuste_alumb, s.otros_afectos, s.base_imponible, s.igv, s.tot_periodo,
      s.electrif_rural, s.comp_distribucion, s.comp_generadora, s.comp_interrup, s.comp_calidad,
      s.comp_frec_ant, s.comp_frec_des, s.comp_serv, s.comp_norma_tec, s.comp_deuda_ant, s.comp_deuda_meses,
      s.comp_dev_reclamo, s.comp_nota_deb_cred, s.comp_aporte_reemb, s.comp_otros_inafectos,
      s.comp_redondeo_act_pos, s.comp_redondeo_act_neg,
      s.costo_medio, s.total_a_pagar, s.valor_venta, s.deuda_anterior, s.devolucion, s.fecha_liquidacion
    FROM stg s
    LEFT JOIN ods.sftp_hm_clientes_libres t
      ON t.num_recibo = s.num_recibo
     AND t.per_consumo = s.per_consumo
    WHERE t.num_recibo IS NULL
    RETURNING 1
  )
  SELECT
    COALESCE( (SELECT count(*) FROM ins), 0 ) AS inserted,
    COALESCE( (SELECT count(*) FROM upd), 0 ) AS updated
  INTO v_inserted, v_updated;

  v_estado := 'DONE';
  v_msj := format('UPSERT hm_clientes_libres (CTE): inserted=%s; updated=%s', v_inserted, v_updated);

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