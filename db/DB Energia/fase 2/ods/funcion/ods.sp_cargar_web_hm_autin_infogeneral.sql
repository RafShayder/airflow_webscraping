-- DROP PROCEDURE ods.sp_cargar_web_hm_autin_infogeneral();

CREATE OR REPLACE PROCEDURE ods.sp_cargar_web_hm_autin_infogeneral()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_inicio   timestamp(0) := clock_timestamp()::timestamp(0);
  v_id_sp    integer      := 'ods.sp_cargar_web_hm_autin_infogeneral()'::regprocedure::oid::int;
  v_sp_name  text         := 'ods.sp_cargar_web_hm_autin_infogeneral()';
  v_inserted integer := 0;
  v_updated  integer := 0;  -- no actualiza
  v_deleted  integer := 0;  -- no borra
  v_nulls    integer := NULL;
  v_estado   varchar(50);
  v_msj      text;
BEGIN
  /*
    1) STG: limpia y castea tipos.
    2) DEDUP: toma 1 fila por task_id (DISTINCT ON).
    3) INSERTA solo los task_id que NO existen en ODS.
  */
  WITH
  stg AS (
    SELECT
      NULLIF(btrim(r.task_id), '') AS task_id,

      /* --------- Timestamp helpers (varios formatos) --------- */
      -- createtime
      CASE
        WHEN NULLIF(btrim(r.createtime),'') IS NULL THEN NULL
        WHEN regexp_replace(r.createtime,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.createtime,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.createtime ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.createtime::date::timestamp
        WHEN r.createtime ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.createtime,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.createtime ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.createtime,'DD/MM/YYYY HH24:MI')
        WHEN r.createtime ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.createtime,'DD/MM/YYYY')::timestamp
        WHEN r.createtime ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.createtime,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.createtime ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.createtime,'DD-MM-YYYY HH24:MI')
        WHEN r.createtime ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.createtime,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS createtime,

      -- dispatch_time
      CASE
        WHEN NULLIF(btrim(r.dispatch_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.dispatch_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.dispatch_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.dispatch_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.dispatch_time::date::timestamp
        WHEN r.dispatch_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.dispatch_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.dispatch_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.dispatch_time,'DD/MM/YYYY HH24:MI')
        WHEN r.dispatch_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.dispatch_time,'DD/MM/YYYY')::timestamp
        WHEN r.dispatch_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.dispatch_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.dispatch_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.dispatch_time,'DD-MM-YYYY HH24:MI')
        WHEN r.dispatch_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.dispatch_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS dispatch_time,

      -- accept_time
      CASE
        WHEN NULLIF(btrim(r.accept_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.accept_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.accept_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.accept_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.accept_time::date::timestamp
        WHEN r.accept_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.accept_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.accept_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.accept_time,'DD/MM/YYYY HH24:MI')
        WHEN r.accept_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.accept_time,'DD/MM/YYYY')::timestamp
        WHEN r.accept_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.accept_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.accept_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.accept_time,'DD-MM-YYYY HH24:MI')
        WHEN r.accept_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.accept_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS accept_time,

      -- depart_time
      CASE
        WHEN NULLIF(btrim(r.depart_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.depart_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.depart_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.depart_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.depart_time::date::timestamp
        WHEN r.depart_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.depart_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.depart_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.depart_time,'DD/MM/YYYY HH24:MI')
        WHEN r.depart_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.depart_time,'DD/MM/YYYY')::timestamp
        WHEN r.depart_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.depart_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.depart_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.depart_time,'DD-MM-YYYY HH24:MI')
        WHEN r.depart_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.depart_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS depart_time,

      -- arrive_time
      CASE
        WHEN NULLIF(btrim(r.arrive_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.arrive_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.arrive_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.arrive_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.arrive_time::date::timestamp
        WHEN r.arrive_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.arrive_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.arrive_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.arrive_time,'DD/MM/YYYY HH24:MI')
        WHEN r.arrive_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.arrive_time,'DD/MM/YYYY')::timestamp
        WHEN r.arrive_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.arrive_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.arrive_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.arrive_time,'DD-MM-YYYY HH24:MI')
        WHEN r.arrive_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.arrive_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS arrive_time,

      -- complete_time
      CASE
        WHEN NULLIF(btrim(r.complete_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.complete_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.complete_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.complete_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.complete_time::date::timestamp
        WHEN r.complete_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.complete_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.complete_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.complete_time,'DD/MM/YYYY HH24:MI')
        WHEN r.complete_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.complete_time,'DD/MM/YYYY')::timestamp
        WHEN r.complete_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.complete_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.complete_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.complete_time,'DD-MM-YYYY HH24:MI')
        WHEN r.complete_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.complete_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS complete_time,

      /* TEXT plano en ODS */
      left(NULLIF(btrim(r.require_finish_time),''), 500) AS require_finish_time,

      -- first_complete_time
      CASE
        WHEN NULLIF(btrim(r.first_complete_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.first_complete_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.first_complete_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.first_complete_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.first_complete_time::date::timestamp
        WHEN r.first_complete_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_complete_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.first_complete_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_complete_time,'DD/MM/YYYY HH24:MI')
        WHEN r.first_complete_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.first_complete_time,'DD/MM/YYYY')::timestamp
        WHEN r.first_complete_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_complete_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.first_complete_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_complete_time,'DD-MM-YYYY HH24:MI')
        WHEN r.first_complete_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.first_complete_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS first_complete_time,

      -- cancel_time
      CASE
        WHEN NULLIF(btrim(r.cancel_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.cancel_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.cancel_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.cancel_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.cancel_time::date::timestamp
        WHEN r.cancel_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.cancel_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.cancel_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.cancel_time,'DD/MM/YYYY HH24:MI')
        WHEN r.cancel_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.cancel_time,'DD/MM/YYYY')::timestamp
        WHEN r.cancel_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.cancel_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.cancel_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.cancel_time,'DD-MM-YYYY HH24:MI')
        WHEN r.cancel_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.cancel_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS cancel_time,

      NULLIF(btrim(r.cancel_operator),'')            AS cancel_operator,
      NULLIF(btrim(r.cancel_reason),'')              AS cancel_reason,
      NULLIF(btrim(r.task_status),'')                AS task_status,
      NULLIF(btrim(r.com_fault_speciality),'')       AS com_fault_speciality,
      NULLIF(btrim(r.com_fault_sub_speciality),'')   AS com_fault_sub_speciality,
      NULLIF(btrim(r.com_fault_cause),'')            AS com_fault_cause,
      NULLIF(btrim(r.leave_observations),'')         AS leave_observations,
      NULLIF(btrim(r.site_id),'')                    AS site_id,
      NULLIF(btrim(r.site_priority),'')              AS site_priority,
      NULLIF(btrim(r.title),'')                      AS title,
      NULLIF(btrim(r.description),'')                AS description,
      NULLIF(btrim(r.nro_toa),'')                    AS nro_toa,
      NULLIF(btrim(r.company_supply),'')             AS company_supply,
      NULLIF(btrim(r.zona),'')                       AS zona,
      NULLIF(btrim(r.departamento),'')               AS departamento,
      NULLIF(btrim(r.torrero),'')                    AS torrero,
      NULLIF(btrim(r.task_type),'')                  AS task_type,
      NULLIF(btrim(r.fm_office),'')                  AS fm_office,
      NULLIF(btrim(r.region),'')                     AS region,
      NULLIF(btrim(r.site_id_name),'')               AS site_id_name,
      NULLIF(btrim(r.site_location_type),'')         AS site_location_type,

      CASE WHEN NULLIF(btrim(r.sla),'') ~ '^-?\d+$'
           THEN r.sla::bigint ELSE NULL END          AS sla,
      NULLIF(btrim(r.sla_status),'')                 AS sla_status,
      NULLIF(btrim(r.suspend_state),'')              AS suspend_state,
      NULLIF(btrim(r.create_operator),'')            AS create_operator,

      -- fault_first_occur_time
      CASE
        WHEN NULLIF(btrim(r.fault_first_occur_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.fault_first_occur_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.fault_first_occur_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.fault_first_occur_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.fault_first_occur_time::date::timestamp
        WHEN r.fault_first_occur_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fault_first_occur_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.fault_first_occur_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fault_first_occur_time,'DD/MM/YYYY HH24:MI')
        WHEN r.fault_first_occur_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.fault_first_occur_time,'DD/MM/YYYY')::timestamp
        WHEN r.fault_first_occur_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fault_first_occur_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.fault_first_occur_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fault_first_occur_time,'DD-MM-YYYY HH24:MI')
        WHEN r.fault_first_occur_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.fault_first_occur_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS fault_first_occur_time,

      NULLIF(btrim(r.schedule_operator),'')          AS schedule_operator,

      -- schedule_time
      CASE
        WHEN NULLIF(btrim(r.schedule_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.schedule_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.schedule_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.schedule_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.schedule_time::date::timestamp
        WHEN r.schedule_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.schedule_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.schedule_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.schedule_time,'DD/MM/YYYY HH24:MI')
        WHEN r.schedule_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.schedule_time,'DD/MM/YYYY')::timestamp
        WHEN r.schedule_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.schedule_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.schedule_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.schedule_time,'DD-MM-YYYY HH24:MI')
        WHEN r.schedule_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.schedule_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS schedule_time,

      NULLIF(btrim(r.assign_to_fme),'')              AS assign_to_fme,
      NULLIF(btrim(r.assign_to_fme_full_name),'')    AS assign_to_fme_full_name,
      NULLIF(btrim(r.dispatch_operator),'')          AS dispatch_operator,
      NULLIF(btrim(r.depart_operator),'')            AS depart_operator,
      NULLIF(btrim(r.arrive_operator),'')            AS arrive_operator,
      NULLIF(btrim(r.complete_operator),'')          AS complete_operator,

      /* TEXT en ODS */
      NULLIF(btrim(r.reject_time),'')                AS reject_time,
      NULLIF(btrim(r.reject_operator),'')            AS reject_operator,
      NULLIF(btrim(r.reject_des),'')                 AS reject_des,
      NULLIF(btrim(r.com_level_1_aff_equip),'')      AS com_level_1_aff_equip,
      NULLIF(btrim(r.com_level_2_aff_equip),'')      AS com_level_2_aff_equip,
      NULLIF(btrim(r.com_level_3_aff_equip),'')      AS com_level_3_aff_equip,
      NULLIF(btrim(r.fault_type),'')                 AS fault_type,
      NULLIF(btrim(r.task_category),'')              AS task_category,
      NULLIF(btrim(r.task_subcategory),'')           AS task_subcategory,
      NULLIF(btrim(r.fme_contrator),'')              AS fme_contrator,
      NULLIF(btrim(r.contratista_sitio),'')          AS contratista_sitio,
      NULLIF(btrim(r.complete_operator_name),'')     AS complete_operator_name,
      NULLIF(btrim(r.reject_operator_name),'')       AS reject_operator_name,
      NULLIF(btrim(r.arrive_operator_name),'')       AS arrive_operator_name,
      NULLIF(btrim(r.depart_operator_name),'')       AS depart_operator_name,
      NULLIF(btrim(r.dispatch_operator_name),'')     AS dispatch_operator_name,
      NULLIF(btrim(r.cancel_operator_name),'')       AS cancel_operator_name,
      NULLIF(btrim(r.schedule_operator_name),'')     AS schedule_operator_name,
      NULLIF(btrim(r.create_operator_name),'')       AS create_operator_name,
      NULLIF(btrim(r.situacion_encontrada),'')       AS situacion_encontrada,
      NULLIF(btrim(r.detalle_de_actuacion_realizada),'') AS detalle_de_actuacion_realizada,
      NULLIF(btrim(r.atencion_incidencias),'')       AS atencion_incidencias,
      NULLIF(btrim(r.remedy_id),'')                  AS remedy_id,

      -- first_pause_time
      CASE
        WHEN NULLIF(btrim(r.first_pause_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.first_pause_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.first_pause_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.first_pause_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.first_pause_time::date::timestamp
        WHEN r.first_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_pause_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.first_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_pause_time,'DD/MM/YYYY HH24:MI')
        WHEN r.first_pause_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.first_pause_time,'DD/MM/YYYY')::timestamp
        WHEN r.first_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_pause_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.first_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_pause_time,'DD-MM-YYYY HH24:MI')
        WHEN r.first_pause_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.first_pause_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS first_pause_time,

      NULLIF(btrim(r.first_pause_operator),'')       AS first_pause_operator,
      NULLIF(btrim(r.first_pause_reason),'')         AS first_pause_reason,
      NULLIF(btrim(r.first_resume_operator),'')      AS first_resume_operator,

      -- first_resume_time
      CASE
        WHEN NULLIF(btrim(r.first_resume_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.first_resume_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.first_resume_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.first_resume_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.first_resume_time::date::timestamp
        WHEN r.first_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_resume_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.first_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_resume_time,'DD/MM/YYYY HH24:MI')
        WHEN r.first_resume_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.first_resume_time,'DD/MM/YYYY')::timestamp
        WHEN r.first_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.first_resume_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.first_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.first_resume_time,'DD-MM-YYYY HH24:MI')
        WHEN r.first_resume_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.first_resume_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS first_resume_time,

      -- second_pause_time
      CASE
        WHEN NULLIF(btrim(r.second_pause_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.second_pause_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.second_pause_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.second_pause_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.second_pause_time::date::timestamp
        WHEN r.second_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.second_pause_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.second_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.second_pause_time,'DD/MM/YYYY HH24:MI')
        WHEN r.second_pause_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.second_pause_time,'DD/MM/YYYY')::timestamp
        WHEN r.second_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.second_pause_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.second_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.second_pause_time,'DD-MM-YYYY HH24:MI')
        WHEN r.second_pause_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.second_pause_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS second_pause_time,

      NULLIF(btrim(r.second_pause_reason),'')        AS second_pause_reason,
      NULLIF(btrim(r.second_pause_operator),'')      AS second_pause_operator,

      -- second_resume_time
      CASE
        WHEN NULLIF(btrim(r.second_resume_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.second_resume_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.second_resume_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.second_resume_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.second_resume_time::date::timestamp
        WHEN r.second_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.second_resume_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.second_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.second_resume_time,'DD/MM/YYYY HH24:MI')
        WHEN r.second_resume_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.second_resume_time,'DD/MM/YYYY')::timestamp
        WHEN r.second_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.second_resume_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.second_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.second_resume_time,'DD-MM-YYYY HH24:MI')
        WHEN r.second_resume_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.second_resume_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS second_resume_time,

      -- *** FIX agregado ***
      NULLIF(btrim(r.second_resume_operator),'')     AS second_resume_operator,

      -- third_pause_time
      CASE
        WHEN NULLIF(btrim(r.third_pause_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.third_pause_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.third_pause_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.third_pause_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.third_pause_time::date::timestamp
        WHEN r.third_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.third_pause_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.third_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.third_pause_time,'DD/MM/YYYY HH24:MI')
        WHEN r.third_pause_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.third_pause_time,'DD/MM/YYYY')::timestamp
        WHEN r.third_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.third_pause_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.third_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.third_pause_time,'DD-MM-YYYY HH24:MI')
        WHEN r.third_pause_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.third_pause_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS third_pause_time,

      NULLIF(btrim(r.third_pause_reason),'')         AS third_pause_reason,
      NULLIF(btrim(r.third_pause_operator),'')       AS third_pause_operator,

      -- third_resume_time
      CASE
        WHEN NULLIF(btrim(r.third_resume_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.third_resume_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.third_resume_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.third_resume_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.third_resume_time::date::timestamp
        WHEN r.third_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.third_resume_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.third_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.third_resume_time,'DD/MM/YYYY HH24:MI')
        WHEN r.third_resume_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.third_resume_time,'DD/MM/YYYY')::timestamp
        WHEN r.third_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.third_resume_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.third_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.third_resume_time,'DD-MM-YYYY HH24:MI')
        WHEN r.third_resume_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.third_resume_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS third_resume_time,

      NULLIF(btrim(r.third_resume_operator),'')      AS third_resume_operator,

      -- fourth_pause_time
      CASE
        WHEN NULLIF(btrim(r.fourth_pause_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.fourth_pause_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.fourth_pause_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.fourth_pause_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.fourth_pause_time::date::timestamp
        WHEN r.fourth_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_pause_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.fourth_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_pause_time,'DD/MM/YYYY HH24:MI')
        WHEN r.fourth_pause_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.fourth_pause_time,'DD/MM/YYYY')::timestamp
        WHEN r.fourth_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_pause_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.fourth_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_pause_time,'DD-MM-YYYY HH24:MI')
        WHEN r.fourth_pause_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.fourth_pause_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS fourth_pause_time,

      NULLIF(btrim(r.fourth_pause_reason),'')        AS fourth_pause_reason,
      NULLIF(btrim(r.fourth_pause_operator),'')      AS fourth_pause_operator,

      -- fourth_resume_time
      CASE
        WHEN NULLIF(btrim(r.fourth_resume_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.fourth_resume_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.fourth_resume_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.fourth_resume_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.fourth_resume_time::date::timestamp
        WHEN r.fourth_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_resume_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.fourth_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_resume_time,'DD/MM/YYYY HH24:MI')
        WHEN r.fourth_resume_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.fourth_resume_time,'DD/MM/YYYY')::timestamp
        WHEN r.fourth_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_resume_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.fourth_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fourth_resume_time,'DD-MM-YYYY HH24:MI')
        WHEN r.fourth_resume_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.fourth_resume_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS fourth_resume_time,

      NULLIF(btrim(r.fourth_resume_operator),'')     AS fourth_resume_operator,

      -- fifth_pause_time
      CASE
        WHEN NULLIF(btrim(r.fifth_pause_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.fifth_pause_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.fifth_pause_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.fifth_pause_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.fifth_pause_time::date::timestamp
        WHEN r.fifth_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_pause_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.fifth_pause_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_pause_time,'DD/MM/YYYY HH24:MI')
        WHEN r.fifth_pause_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.fifth_pause_time,'DD/MM/YYYY')::timestamp
        WHEN r.fifth_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_pause_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.fifth_pause_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_pause_time,'DD-MM-YYYY HH24:MI')
        WHEN r.fifth_pause_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.fifth_pause_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS fifth_pause_time,

      NULLIF(btrim(r.fifth_pause_reason),'')         AS fifth_pause_reason,
      NULLIF(btrim(r.fifth_pause_operator),'')       AS fifth_pause_operator,

      -- fifth_resume_time
      CASE
        WHEN NULLIF(btrim(r.fifth_resume_time),'') IS NULL THEN NULL
        WHEN regexp_replace(r.fifth_resume_time,'Z$','') ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'
          THEN replace(split_part(regexp_replace(r.fifth_resume_time,'Z$',''),'.',1),'T',' ')::timestamp
        WHEN r.fifth_resume_time ~ '^\d{4}-\d{2}-\d{2}$'
          THEN r.fifth_resume_time::date::timestamp
        WHEN r.fifth_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_resume_time,'DD/MM/YYYY HH24:MI:SS')
        WHEN r.fifth_resume_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_resume_time,'DD/MM/YYYY HH24:MI')
        WHEN r.fifth_resume_time ~ '^\d{2}/\d{2}/\d{4}$'
          THEN to_date(r.fifth_resume_time,'DD/MM/YYYY')::timestamp
        WHEN r.fifth_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_resume_time,'DD-MM-YYYY HH24:MI:SS')
        WHEN r.fifth_resume_time ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
          THEN to_timestamp(r.fifth_resume_time,'DD-MM-YYYY HH24:MI')
        WHEN r.fifth_resume_time ~ '^\d{2}-\d{2}-\d{4}$'
          THEN to_date(r.fifth_resume_time,'DD-MM-YYYY')::timestamp
        ELSE NULL
      END AS fifth_resume_time,

      NULLIF(btrim(r.fifth_resume_operator),'')      AS fifth_resume_operator,

      NULLIF(btrim(r.reject_flag),'')                AS reject_flag,
      CASE WHEN NULLIF(btrim(r.reject_counter),'') ~ '^-?\d+$'
           THEN r.reject_counter::bigint ELSE NULL END AS reject_counter,
      NULLIF(btrim(r.fuel_type),'')                  AS fuel_type
    FROM raw.web_mm_autin_infogeneral r
    WHERE NULLIF(btrim(r.task_id),'') IS NOT NULL
  ),
  dedup AS (
    SELECT DISTINCT ON (task_id) *
    FROM stg
    ORDER BY task_id
  ),
  ins AS (
    INSERT INTO ods.web_hm_autin_infogeneral (
      task_id, createtime, dispatch_time, accept_time, depart_time, arrive_time, complete_time,
      require_finish_time, first_complete_time, cancel_time, cancel_operator, cancel_reason, task_status,
      com_fault_speciality, com_fault_sub_speciality, com_fault_cause, leave_observations, site_id,
      site_priority, title, description, nro_toa, company_supply, zona, departamento, torrero, task_type,
      fm_office, region, site_id_name, site_location_type, sla, sla_status, suspend_state, create_operator,
      fault_first_occur_time, schedule_operator, schedule_time, assign_to_fme, assign_to_fme_full_name,
      dispatch_operator, depart_operator, arrive_operator, complete_operator, reject_time, reject_operator,
      reject_des, com_level_1_aff_equip, com_level_2_aff_equip, com_level_3_aff_equip, fault_type,
      task_category, task_subcategory, fme_contrator, contratista_sitio, complete_operator_name,
      reject_operator_name, arrive_operator_name, depart_operator_name, dispatch_operator_name,
      cancel_operator_name, schedule_operator_name, create_operator_name, situacion_encontrada,
      detalle_de_actuacion_realizada, atencion_incidencias, remedy_id, first_pause_time, first_pause_operator,
      first_pause_reason, first_resume_operator, first_resume_time, second_pause_time, second_pause_reason,
      second_pause_operator, second_resume_time, second_resume_operator, third_pause_time, third_pause_reason,
      third_pause_operator, third_resume_time, third_resume_operator, fourth_pause_time, fourth_pause_reason,
      fourth_pause_operator, fourth_resume_time, fourth_resume_operator, fifth_pause_time, fifth_pause_reason,
      fifth_pause_operator, fifth_resume_time, fifth_resume_operator, reject_flag, reject_counter, fuel_type
    )
    SELECT
      s.task_id, s.createtime, s.dispatch_time, s.accept_time, s.depart_time, s.arrive_time, s.complete_time,
      s.require_finish_time, s.first_complete_time, s.cancel_time, s.cancel_operator, s.cancel_reason, s.task_status,
      s.com_fault_speciality, s.com_fault_sub_speciality, s.com_fault_cause, s.leave_observations, s.site_id,
      s.site_priority, s.title, s.description, s.nro_toa, s.company_supply, s.zona, s.departamento, s.torrero, s.task_type,
      s.fm_office, s.region, s.site_id_name, s.site_location_type, s.sla, s.sla_status, s.suspend_state, s.create_operator,
      s.fault_first_occur_time, s.schedule_operator, s.schedule_time, s.assign_to_fme, s.assign_to_fme_full_name,
      s.dispatch_operator, s.depart_operator, s.arrive_operator, s.complete_operator, s.reject_time, s.reject_operator,
      s.reject_des, s.com_level_1_aff_equip, s.com_level_2_aff_equip, s.com_level_3_aff_equip, s.fault_type,
      s.task_category, s.task_subcategory, s.fme_contrator, s.contratista_sitio, s.complete_operator_name,
      s.reject_operator_name, s.arrive_operator_name, s.depart_operator_name, s.dispatch_operator_name,
      s.cancel_operator_name, s.schedule_operator_name, s.create_operator_name, s.situacion_encontrada,
      s.detalle_de_actuacion_realizada, s.atencion_incidencias, s.remedy_id, s.first_pause_time, s.first_pause_operator,
      s.first_pause_reason, s.first_resume_operator, s.first_resume_time, s.second_pause_time, s.second_pause_reason,
      s.second_pause_operator, s.second_resume_time, s.second_resume_operator, s.third_pause_time, s.third_pause_reason,
      s.third_pause_operator, s.third_resume_time, s.third_resume_operator, s.fourth_pause_time, s.fourth_pause_reason,
      s.fourth_pause_operator, s.fourth_resume_time, s.fourth_resume_operator, s.fifth_pause_time, s.fifth_pause_reason,
      s.fifth_pause_operator, s.fifth_resume_time, s.fifth_resume_operator, s.reject_flag, s.reject_counter, s.fuel_type
    FROM dedup s
    LEFT JOIN ods.web_hm_autin_infogeneral t
      ON t.task_id = s.task_id
    WHERE t.task_id IS NULL
    RETURNING 1
  )
  SELECT COALESCE((SELECT COUNT(*) FROM ins), 0)
  INTO v_inserted;

  v_estado := 'DONE';
  v_msj := format('Insertados en ODS: %s | Upd: %s | Del: %s | Origen: raw.web_mm_autin_infogeneral',
                  v_inserted, v_updated, v_deleted);

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
