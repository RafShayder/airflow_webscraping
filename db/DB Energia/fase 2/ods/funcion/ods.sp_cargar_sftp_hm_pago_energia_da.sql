-- DROP PROCEDURE ods.sp_cargar_sftp_hm_pago_energia_da();

CREATE OR REPLACE PROCEDURE ods.sp_cargar_sftp_hm_pago_energia_da()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio        timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp         integer      := 'ods.sp_cargar_sftp_hm_pago_energia_da()'::regprocedure::oid::int;
  v_sp_name       text         := 'ods.sp_cargar_sftp_hm_pago_energia_da()'::regprocedure::text;

  v_inserted      integer := 0;
  v_err_dups      integer := 0;
  v_err_nulos     integer := 0;
  v_err_total     integer := 0;

  v_estado        varchar(50);
  v_msj           text;
BEGIN
  /* ========= 1.a) NULOS / VACÍOS en RECIBO ========= */
  WITH cand AS (
    SELECT r.*
    FROM raw.sftp_mm_pago_energia_da r
    WHERE r.recibo IS NULL
       OR btrim(r.recibo) = ''
       OR lower(btrim(r.recibo)) IN ('nan','null')
  )
  INSERT INTO public.error_pago_energia (
    contador_pedido, organizacion_compra, grupo_compra, sociedad,
    nota_al_aprobador_char_40, nombre_pedido_char_40, material, texto_breve_del_material,
    cantidad, fecha_de_entrega, ruc_proveedor, condicion_de_pago, centro, tipo_posicion,
    tipo_imputacion, centro_costo, orden, elemento_pep, area_funcional, linea_negocio,
    segmento, grupo_segmento, sub_segmento, indicador_de_impuestos, moneda, licitacion,
    precio_neto, solicitante, uno, dos, tres, cuatro, cinco, seis, siete, ocho, nueve, diez, once,
    proveedor_flm, numero_de_hoja, concesionario, ruc_concesionario, tercero,
    tipo_recibo, zonal, cliente, numero_documento, numero_suministro, nombre_local, direccion_local,
    servicio, recibo, original, tipo_recibo_2, tipo_voucher, porcentaje_voucher,
    importe_voucher, base_imponible, pago_inafectos, valor_venta_total, igv, total_pagado,
    fecha_emision, fecha_vencimiento, periodo_consumo, mes_facturacion, periodo_reporte,
    observacion, clase, numero_sap, neteo, cesta, orden_de_compra, certificacion,
    soles_comparativo, unnamed_76, codigo_unico_local, cuenta, fe_valor, mon,
    importe_en_md, texto, asignacion, deuda_mes_anterior, total_pagado_recibo,
    fecha_carga, tabla_origen
  )
  SELECT
    r.contador_pedido, r.organizacion_compra, r.grupo_compra, r.sociedad,
    r.nota_al_aprobador_char_40, r.nombre_pedido_char_40, r.material, r.texto_breve_del_material,
    r.cantidad, r.fecha_de_entrega, r.ruc_proveedor, r.condicion_de_pago, r.centro, r.tipo_posicion,
    r.tipo_imputacion, r.centro_costo, r.orden, r.elemento_pep, r.area_funcional, r.linea_negocio,
    r.segmento, r.grupo_segmento, r.sub_segmento, r.indicador_de_impuestos, r.moneda, r.licitacion,
    r.precio_neto, r.solicitante, r.uno, r.dos, r.tres, r.cuatro, r.cinco, r.seis, r.siete, r.ocho, r.nueve, r.diez, r.once,
    r.proveedor_flm, r.numero_de_hoja, r.concesionario, r.ruc_concesionario, r.tercero,
    r.tipo_recibo, r.zonal, r.cliente, r.numero_documento, r.numero_suministro, r.nombre_local, r.direccion_local,
    r.servicio, r.recibo, r.original, r.tipo_recibo_2, r.tipo_voucher, r.porcentaje_voucher,
    r.importe_voucher, r.base_imponible, r.pago_inafectos, r.valor_venta_total, r.igv, r.total_pagado,
    r.fecha_emision, r.fecha_vencimiento, r.periodo_consumo, r.mes_facturacion, r.periodo_reporte,
    r.observacion, r.clase, r.numero_sap, r.neteo, r.cesta, r.orden_de_compra, r.certificacion,
    r.soles_comparativo, r.unnamed_76, r.codigo_unico_local, r.cuenta, r.fe_valor, r.mon,
    r.importe_en_md, r.texto, r.asignacion, r.deuda_mes_anterior, r.total_pagado_recibo,
    clock_timestamp(), 'raw.sftp_mm_pago_energia_da'
  FROM cand r
  LEFT JOIN public.error_pago_energia e
         ON ( (e.recibo IS NULL AND r.recibo IS NULL) OR e.recibo = r.recibo )
        AND e.tabla_origen = 'raw.sftp_mm_pago_energia_da'
        AND e.fecha_carga::date = clock_timestamp()::date
  WHERE e.fecha_carga IS NULL;

  GET DIAGNOSTICS v_err_nulos = ROW_COUNT;

  -- Borro NULOS/VACÍOS de RAW
  WITH rid_nulos AS (
    SELECT r.ctid AS rid
    FROM raw.sftp_mm_pago_energia_da r
    WHERE r.recibo IS NULL
       OR btrim(r.recibo) = ''
       OR lower(btrim(r.recibo)) IN ('nan','null')
  )
  DELETE FROM raw.sftp_mm_pago_energia_da r
  USING rid_nulos x
  WHERE r.ctid = x.rid;

  /* ========= 1.b) DUPLICADOS EXACTOS por RECIBO ========= */
  WITH claves_dup AS (
    SELECT r.recibo
    FROM raw.sftp_mm_pago_energia_da r
    GROUP BY r.recibo
    HAVING COUNT(*) > 1
  ),
  cand_dups AS (
    SELECT r.*
    FROM raw.sftp_mm_pago_energia_da r
    JOIN claves_dup d ON d.recibo = r.recibo
  )
  INSERT INTO public.error_pago_energia (
    contador_pedido, organizacion_compra, grupo_compra, sociedad,
    nota_al_aprobador_char_40, nombre_pedido_char_40, material, texto_breve_del_material,
    cantidad, fecha_de_entrega, ruc_proveedor, condicion_de_pago, centro, tipo_posicion,
    tipo_imputacion, centro_costo, orden, elemento_pep, area_funcional, linea_negocio,
    segmento, grupo_segmento, sub_segmento, indicador_de_impuestos, moneda, licitacion,
    precio_neto, solicitante, uno, dos, tres, cuatro, cinco, seis, siete, ocho, nueve, diez, once,
    proveedor_flm, numero_de_hoja, concesionario, ruc_concesionario, tercero,
    tipo_recibo, zonal, cliente, numero_documento, numero_suministro, nombre_local, direccion_local,
    servicio, recibo, original, tipo_recibo_2, tipo_voucher, porcentaje_voucher,
    importe_voucher, base_imponible, pago_inafectos, valor_venta_total, igv, total_pagado,
    fecha_emision, fecha_vencimiento, periodo_consumo, mes_facturacion, periodo_reporte,
    observacion, clase, numero_sap, neteo, cesta, orden_de_compra, certificacion,
    soles_comparativo, unnamed_76, codigo_unico_local, cuenta, fe_valor, mon,
    importe_en_md, texto, asignacion, deuda_mes_anterior, total_pagado_recibo,
    fecha_carga, tabla_origen
  )
  SELECT
    r.contador_pedido, r.organizacion_compra, r.grupo_compra, r.sociedad,
    r.nota_al_aprobador_char_40, r.nombre_pedido_char_40, r.material, r.texto_breve_del_material,
    r.cantidad, r.fecha_de_entrega, r.ruc_proveedor, r.condicion_de_pago, r.centro, r.tipo_posicion,
    r.tipo_imputacion, r.centro_costo, r.orden, r.elemento_pep, r.area_funcional, r.linea_negocio,
    r.segmento, r.grupo_segmento, r.sub_segmento, r.indicador_de_impuestos, r.moneda, r.licitacion,
    r.precio_neto, r.solicitante, r.uno, r.dos, r.tres, r.cuatro, r.cinco, r.seis, r.siete, r.ocho, r.nueve, r.diez, r.once,
    r.proveedor_flm, r.numero_de_hoja, r.concesionario, r.ruc_concesionario, r.tercero,
    r.tipo_recibo, r.zonal, r.cliente, r.numero_documento, r.numero_suministro, r.nombre_local, r.direccion_local,
    r.servicio, r.recibo, r.original, r.tipo_recibo_2, r.tipo_voucher, r.porcentaje_voucher,
    r.importe_voucher, r.base_imponible, r.pago_inafectos, r.valor_venta_total, r.igv, r.total_pagado,
    r.fecha_emision, r.fecha_vencimiento, r.periodo_consumo, r.mes_facturacion, r.periodo_reporte,
    r.observacion, r.clase, r.numero_sap, r.neteo, r.cesta, r.orden_de_compra, r.certificacion,
    r.soles_comparativo, r.unnamed_76, r.codigo_unico_local, r.cuenta, r.fe_valor, r.mon,
    r.importe_en_md, r.texto, r.asignacion, r.deuda_mes_anterior, r.total_pagado_recibo,
    clock_timestamp(), 'raw.sftp_mm_pago_energia_da'
  FROM cand_dups r
  LEFT JOIN public.error_pago_energia e
         ON e.recibo = r.recibo
        AND e.tabla_origen = 'raw.sftp_mm_pago_energia_da'
        AND e.fecha_carga::date = clock_timestamp()::date
  WHERE e.recibo IS NULL;

  GET DIAGNOSTICS v_err_dups = ROW_COUNT;

  -- Borro duplicados por RECIBO de RAW (se envían sólo a error)
  WITH claves_dup AS (
    SELECT r.recibo
    FROM raw.sftp_mm_pago_energia_da r
    GROUP BY r.recibo
    HAVING COUNT(*) > 1
  ),
  rid_dup AS (
    SELECT r.ctid AS rid
    FROM raw.sftp_mm_pago_energia_da r
    JOIN claves_dup d ON d.recibo = r.recibo
  )
  DELETE FROM raw.sftp_mm_pago_energia_da r
  USING rid_dup x
  WHERE r.ctid = x.rid;

  v_err_total := COALESCE(v_err_dups,0) + COALESCE(v_err_nulos,0);

  /* ========= 2) TRANSFORMACIÓN + DEDUP + CARGA A ODS ========= */
  WITH base AS (
    SELECT
      r.ctid AS rid,

      trunc(public.try_numeric(r.contador_pedido))::bigint    AS contador_pedido,
      NULLIF(NULLIF(btrim(r.organizacion_compra),'NaN'),'')::varchar(250) AS organizacion_compra,
      trunc(public.try_numeric(r.grupo_compra))::bigint       AS grupo_compra,
      trunc(public.try_numeric(r.sociedad))::bigint           AS sociedad,
      NULLIF(NULLIF(btrim(r.nota_al_aprobador_char_40),'NaN'),'')::varchar(250) AS nota_al_aprobador_char_40,
      NULLIF(NULLIF(btrim(r.nombre_pedido_char_40),'NaN'),'')::varchar(250)    AS nombre_pedido_char_40,
      trunc(public.try_numeric(r.material))::bigint           AS material,
      NULLIF(NULLIF(btrim(r.texto_breve_del_material),'NaN'),'')::varchar(255) AS texto_breve_del_material,
      trunc(public.try_numeric(r.cantidad))::bigint           AS cantidad,
      trunc(public.try_numeric(r.fecha_de_entrega))::bigint   AS fecha_de_entrega,   -- en ODS es int8
      trunc(public.try_numeric(r.ruc_proveedor))::bigint      AS ruc_proveedor,
      NULLIF(NULLIF(btrim(r.condicion_de_pago),'NaN'),'')::varchar(250)  AS condicion_de_pago,
      NULLIF(NULLIF(btrim(r.centro),'NaN'),'')::varchar(255)  AS centro,
      NULLIF(NULLIF(btrim(r.tipo_posicion),'NaN'),'')::varchar(250)      AS tipo_posicion,
      NULLIF(NULLIF(btrim(r.tipo_imputacion),'NaN'),'')::varchar(250)    AS tipo_imputacion,
      NULLIF(NULLIF(btrim(r.centro_costo),'NaN'),'')::varchar(250)       AS centro_costo,
      trunc(public.try_numeric(r.orden))::bigint              AS orden,
      NULLIF(NULLIF(btrim(r.elemento_pep),'NaN'),'')::varchar(250)       AS elemento_pep,
      NULLIF(NULLIF(btrim(r.area_funcional),'NaN'),'')::varchar(255)     AS area_funcional,
      NULLIF(NULLIF(btrim(r.linea_negocio),'NaN'),'')::varchar(255)      AS linea_negocio,
      NULLIF(NULLIF(btrim(r.segmento),'NaN'),'')::varchar(255)           AS segmento,
      NULLIF(NULLIF(btrim(r.grupo_segmento),'NaN'),'')::varchar(255)     AS grupo_segmento,
      NULLIF(NULLIF(btrim(r.sub_segmento),'NaN'),'')::varchar(255)       AS sub_segmento,
      NULLIF(NULLIF(btrim(r.indicador_de_impuestos),'NaN'),'')::varchar(255) AS indicador_de_impuestos,
      NULLIF(NULLIF(btrim(r.moneda),'NaN'),'')::varchar(250)  AS moneda,
      NULLIF(NULLIF(btrim(r.licitacion),'NaN'),'')::varchar(250) AS licitacion,
      public.try_numeric(r.precio_neto)::numeric(18,2)        AS precio_neto,
      NULLIF(NULLIF(btrim(r.solicitante),'NaN'),'')::varchar(255) AS solicitante,
      NULLIF(NULLIF(btrim(r.uno),'NaN'),'')::varchar(255)     AS uno,
      NULLIF(NULLIF(btrim(r.dos),'NaN'),'')::varchar(255)     AS dos,
      NULLIF(NULLIF(btrim(r.tres),'NaN'),'')::varchar(255)    AS tres,
      NULLIF(NULLIF(btrim(r.cuatro),'NaN'),'')::varchar(255)  AS cuatro,
      NULLIF(NULLIF(btrim(r.cinco),'NaN'),'')::varchar(255)   AS cinco,
      NULLIF(NULLIF(btrim(r.seis),'NaN'),'')::varchar(255)    AS seis,
      NULLIF(NULLIF(btrim(r.siete),'NaN'),'')::varchar(255)   AS siete,
      NULLIF(NULLIF(btrim(r.ocho),'NaN'),'')::varchar(255)    AS ocho,
      NULLIF(NULLIF(btrim(r.nueve),'NaN'),'')::varchar(255)   AS nueve,
      NULLIF(NULLIF(btrim(r.diez),'NaN'),'')::varchar(255)    AS diez,
      NULLIF(NULLIF(btrim(r.once),'NaN'),'')::varchar(255)    AS once,
      NULLIF(NULLIF(btrim(r.proveedor_flm),'NaN'),'')::varchar(255) AS proveedor_flm,
      NULLIF(NULLIF(btrim(r.numero_de_hoja),'NaN'),'')::varchar(255)   AS numero_de_hoja,
      NULLIF(NULLIF(btrim(r.concesionario),'NaN'),'')::varchar(255)    AS concesionario,
      trunc(public.try_numeric(r.ruc_concesionario))::bigint  AS ruc_concesionario,
      NULLIF(NULLIF(btrim(r.tercero),'NaN'),'')::varchar(255) AS tercero,
      NULLIF(NULLIF(btrim(r.tipo_recibo),'NaN'),'')::varchar(255)  AS tipo_recibo,
      NULLIF(NULLIF(btrim(r.zonal),'NaN'),'')::varchar(255)   AS zonal,
      NULLIF(NULLIF(btrim(r.cliente),'NaN'),'')::varchar(255) AS cliente,
      NULLIF(NULLIF(btrim(r.numero_documento),'NaN'),'')::varchar(255)  AS numero_documento,
      NULLIF(NULLIF(btrim(r.numero_suministro),'NaN'),'')::varchar(255) AS numero_suministro,
      NULLIF(NULLIF(btrim(r.nombre_local),'NaN'),'')::varchar(255)      AS nombre_local,
      NULLIF(NULLIF(btrim(r.direccion_local),'NaN'),'')::varchar(255)   AS direccion_local,
      NULLIF(NULLIF(btrim(r.servicio),'NaN'),'')::varchar(255)          AS servicio,

      CASE
        WHEN r.recibo IS NULL OR btrim(r.recibo) = '' OR lower(btrim(r.recibo)) IN ('nan','null') THEN NULL
        ELSE left(btrim(r.recibo), 255)
      END::varchar(255) AS recibo,

      NULLIF(NULLIF(btrim(r.original),'NaN'),'')::varchar(255)       AS original,
      NULLIF(NULLIF(btrim(r.tipo_recibo_2),'NaN'),'')::varchar(255)  AS tipo_recibo_2,
      NULLIF(NULLIF(btrim(r.tipo_voucher),'NaN'),'')::varchar(255)   AS tipo_voucher,
      NULLIF(NULLIF(btrim(r.porcentaje_voucher),'NaN'),'')::varchar(255) AS porcentaje_voucher,

      public.try_numeric(r.importe_voucher)::numeric(18,2)    AS importe_voucher,
      public.try_numeric(r.base_imponible)::numeric(18,2)     AS base_imponible,
      public.try_numeric(r.pago_inafectos)::numeric(18,2)     AS pago_inafectos,
      public.try_numeric(r.valor_venta_total)::numeric(18,2)  AS valor_venta_total,
      public.try_numeric(r.igv)::numeric(18,2)                AS igv,
      public.try_numeric(r.total_pagado)::numeric(18,2)       AS total_pagado,

      CASE
        WHEN r.fecha_emision IS NULL OR btrim(r.fecha_emision) = '' OR lower(btrim(r.fecha_emision)) IN ('nan','null') THEN NULL
        WHEN position('/' in r.fecha_emision) > 0 THEN to_date(split_part(r.fecha_emision,' ',1), 'DD/MM/YYYY')
        ELSE to_date(split_part(r.fecha_emision,' ',1), 'YYYY-MM-DD')
      END AS fecha_emision,

      CASE
        WHEN r.fecha_vencimiento IS NULL OR btrim(r.fecha_vencimiento) = '' OR lower(btrim(r.fecha_vencimiento)) IN ('nan','null') THEN NULL
        WHEN position('/' in r.fecha_vencimiento) > 0 THEN to_date(split_part(r.fecha_vencimiento,' ',1), 'DD/MM/YYYY')
        ELSE to_date(split_part(r.fecha_vencimiento,' ',1), 'YYYY-MM-DD')
      END AS fecha_vencimiento,

      NULLIF(NULLIF(btrim(r.periodo_consumo),'NaN'),'')::varchar(255)  AS periodo_consumo,
      NULLIF(NULLIF(btrim(r.mes_facturacion),'NaN'),'')::varchar(255)  AS mes_facturacion,
      NULLIF(NULLIF(btrim(r.periodo_reporte),'NaN'),'')::varchar(255)  AS periodo_reporte,
      NULLIF(NULLIF(btrim(r.observacion),'NaN'),'')::varchar(255)      AS observacion,
      NULLIF(NULLIF(btrim(r.clase),'NaN'),'')::varchar(255)            AS clase,
      NULLIF(NULLIF(btrim(r.numero_sap),'NaN'),'')::varchar(255)       AS numero_sap,
      NULLIF(NULLIF(btrim(r.neteo),'NaN'),'')::varchar(255)            AS neteo,
      NULLIF(NULLIF(btrim(r.cesta),'NaN'),'')::varchar(255)            AS cesta,
      NULLIF(NULLIF(btrim(r.orden_de_compra),'NaN'),'')::varchar(255)  AS orden_de_compra,
      NULLIF(NULLIF(btrim(r.certificacion),'NaN'),'')::varchar(255)    AS certificacion,

      trunc(public.try_numeric(r.soles_comparativo))::bigint   AS soles_comparativo,
      NULLIF(NULLIF(btrim(r.unnamed_76),'NaN'),'')::varchar(250) AS unnamed_76,
      trunc(public.try_numeric(r.codigo_unico_local))::bigint  AS codigo_unico_local,
      trunc(public.try_numeric(r.cuenta))::bigint              AS cuenta,

      CASE
        WHEN r.fe_valor IS NULL OR btrim(r.fe_valor) = '' OR lower(btrim(r.fe_valor)) IN ('nan','null') THEN NULL
        WHEN position('/' in r.fe_valor) > 0 THEN to_date(split_part(r.fe_valor,' ',1), 'DD/MM/YYYY')
        ELSE to_date(split_part(r.fe_valor,' ',1), 'YYYY-MM-DD')
      END AS fe_valor,

      NULLIF(NULLIF(btrim(r.mon),'NaN'),'')::varchar(250)      AS mon,
      public.try_numeric(r.importe_en_md)::numeric(18,2)       AS importe_en_md,
      NULLIF(btrim(r.texto),'')::text                          AS texto,
      NULLIF(NULLIF(btrim(r.asignacion),'NaN'),'')::varchar(250) AS asignacion,
      public.try_numeric(r.deuda_mes_anterior)::numeric(18,2)  AS deuda_mes_anterior,
      trunc(public.try_numeric(r.total_pagado_recibo))::bigint AS total_pagado_recibo

    FROM raw.sftp_mm_pago_energia_da r
  ),
  dedup AS (
    -- PK ODS: recibo (conserva primera aparición)
    SELECT DISTINCT ON (recibo) *
    FROM base
    WHERE recibo IS NOT NULL
    ORDER BY recibo, rid
  )
  INSERT INTO ods.sftp_hm_pago_energia (
    contador_pedido, organizacion_compra, grupo_compra, sociedad,
    nota_al_aprobador_char_40, nombre_pedido_char_40, material, texto_breve_del_material,
    cantidad, fecha_de_entrega, ruc_proveedor, condicion_de_pago, centro, tipo_posicion,
    tipo_imputacion, centro_costo, orden, elemento_pep, area_funcional, linea_negocio,
    segmento, grupo_segmento, sub_segmento, indicador_de_impuestos, moneda, licitacion,
    precio_neto, solicitante, uno, dos, tres, cuatro, cinco, seis, siete, ocho, nueve, diez, once,
    proveedor_flm, numero_de_hoja, concesionario, ruc_concesionario, tercero,
    tipo_recibo, zonal, cliente, numero_documento, numero_suministro, nombre_local, direccion_local,
    servicio, recibo, original, tipo_recibo_2, tipo_voucher, porcentaje_voucher,
    importe_voucher, base_imponible, pago_inafectos, valor_venta_total, igv, total_pagado,
    fecha_emision, fecha_vencimiento, periodo_consumo, mes_facturacion, periodo_reporte,
    observacion, clase, numero_sap, neteo, cesta, orden_de_compra, certificacion,
    soles_comparativo, unnamed_76, codigo_unico_local, cuenta, fe_valor, mon,
    importe_en_md, texto, asignacion, deuda_mes_anterior, total_pagado_recibo
  )
  SELECT
    d.contador_pedido, d.organizacion_compra, d.grupo_compra, d.sociedad,
    d.nota_al_aprobador_char_40, d.nombre_pedido_char_40, d.material, d.texto_breve_del_material,
    d.cantidad, d.fecha_de_entrega, d.ruc_proveedor, d.condicion_de_pago, d.centro, d.tipo_posicion,
    d.tipo_imputacion, d.centro_costo, d.orden, d.elemento_pep, d.area_funcional, d.linea_negocio,
    d.segmento, d.grupo_segmento, d.sub_segmento, d.indicador_de_impuestos, d.moneda, d.licitacion,
    d.precio_neto, d.solicitante, d.uno, d.dos, d.tres, d.cuatro, d.cinco, d.seis, d.siete, d.ocho, d.nueve, d.diez, d.once,
    d.proveedor_flm, d.numero_de_hoja, d.concesionario, d.ruc_concesionario, d.tercero,
    d.tipo_recibo, d.zonal, d.cliente, d.numero_documento, d.numero_suministro, d.nombre_local, d.direccion_local,
    d.servicio, d.recibo, d.original, d.tipo_recibo_2, d.tipo_voucher, d.porcentaje_voucher,
    d.importe_voucher, d.base_imponible, d.pago_inafectos, d.valor_venta_total, d.igv, d.total_pagado,
    d.fecha_emision, d.fecha_vencimiento, d.periodo_consumo, d.mes_facturacion, d.periodo_reporte,
    d.observacion, d.clase, d.numero_sap, d.neteo, d.cesta, d.orden_de_compra, d.certificacion,
    d.soles_comparativo, d.unnamed_76, d.codigo_unico_local, d.cuenta, d.fe_valor, d.mon,
    d.importe_en_md, d.texto, d.asignacion, d.deuda_mes_anterior, d.total_pagado_recibo
  FROM dedup d
  LEFT JOIN ods.sftp_hm_pago_energia o
    ON o.recibo = d.recibo
  WHERE o.recibo IS NULL;

  GET DIAGNOSTICS v_inserted = ROW_COUNT;

  v_estado := 'DONE';
  v_msj := format(
    'Insertados en ODS: %s | Enviados a public.error_pago_energia (hoy): %s (duplicados recibo: %s, nulos/vacíos recibo: %s) | Origen: raw.sftp_mm_pago_energia_da.',
    v_inserted, COALESCE(v_err_total,0), COALESCE(v_err_dups,0), COALESCE(v_err_nulos,0)
  );

  CALL public.sp_grabar_log_sp(
    p_id_sp      => v_id_sp,
    p_inicio     => v_inicio,
    p_fin        => clock_timestamp()::timestamp(0),
    p_inserted   => v_inserted,
    p_updated    => NULL::integer,
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
