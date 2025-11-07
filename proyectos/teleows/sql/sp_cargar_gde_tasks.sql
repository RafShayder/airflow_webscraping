-- ============================================================================
-- Stored Procedure: ods.sp_cargar_gde_tasks
-- Descripción: Transforma datos de GDE desde RAW (VARCHAR) hacia ODS (tipos correctos)
-- Autor: ETL System
-- Fecha: 2025-10-30
-- ============================================================================

-- Paso 1: Crear esquemas si no existen
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS public;

-- ============================================================================
-- TABLA RAW: Datos crudos del Excel (todos VARCHAR)
-- ============================================================================

DROP TABLE IF EXISTS raw.gde_tasks CASCADE;

CREATE TABLE raw.gde_tasks (
    task_id VARCHAR(255),
    remedy_id VARCHAR(255),

    -- Timestamps (como VARCHAR en RAW)
    create_time VARCHAR(255),
    dispatch_time VARCHAR(255),
    accept_time VARCHAR(255),
    depart_time VARCHAR(255),
    arrive_time VARCHAR(255),
    complete_time VARCHAR(255),
    require_finish_time VARCHAR(255),
    first_complete_time VARCHAR(255),
    cancel_time VARCHAR(255),

    -- Información de cancelación
    cancel_operator VARCHAR(255),
    cancel_reason VARCHAR(255),

    -- Estado y prioridad
    task_status VARCHAR(255),
    site_priority VARCHAR(255),
    sla_status VARCHAR(255),
    suspend_state VARCHAR(255),

    -- Información del sitio
    site_id VARCHAR(255),
    site_id_name VARCHAR(255),
    site_location_type VARCHAR(255),
    title TEXT,
    description TEXT,

    -- Información geográfica
    zona VARCHAR(255),
    departamento VARCHAR(255),
    region VARCHAR(255),

    -- Información de la tarea
    task_type VARCHAR(255),
    task_category VARCHAR(255),
    task_subcategory VARCHAR(255),
    nro_toa VARCHAR(255),

    -- SLA (como VARCHAR en RAW)
    sla VARCHAR(255),

    -- Operadores
    create_operator VARCHAR(255),
    create_operator_name VARCHAR(255),
    schedule_operator VARCHAR(255),
    schedule_operator_name VARCHAR(255),
    schedule_time VARCHAR(255),
    assign_to_fme VARCHAR(255),
    assign_to_fme_full_name VARCHAR(255),
    dispatch_operator VARCHAR(255),
    dispatch_operator_name VARCHAR(255),
    depart_operator VARCHAR(255),
    depart_operator_name VARCHAR(255),
    arrive_operator VARCHAR(255),
    arrive_operator_name VARCHAR(255),
    complete_operator VARCHAR(255),
    complete_operator_name VARCHAR(255),

    -- Información de rechazo
    reject_time VARCHAR(255),
    reject_operator VARCHAR(255),
    reject_operator_name VARCHAR(255),
    reject_des VARCHAR(255),
    reject_flag VARCHAR(255),
    reject_counter VARCHAR(255),

    -- Información de fallas
    com_fault_speciality VARCHAR(255),
    com_fault_sub_speciality VARCHAR(255),
    com_fault_cause VARCHAR(255),
    fault_first_occur_time VARCHAR(255),
    fault_type VARCHAR(255),

    -- Equipos afectados
    com_level_1_aff_equip VARCHAR(255),
    com_level_2_aff_equip VARCHAR(255),
    com_level_3_aff_equip VARCHAR(255),

    -- Observaciones
    leave_observations TEXT,
    situacion_encontrada TEXT,
    detalle_actuacion_realizada TEXT,

    -- Información de contratistas
    fme_contractor VARCHAR(255),
    contratista_sitio VARCHAR(255),
    company_supply VARCHAR(255),
    torrero VARCHAR(255),
    fm_office VARCHAR(255),

    -- Información de incidencias
    atencion_incidencias VARCHAR(255),

    -- Pausas y reanudaciones
    first_pause_time VARCHAR(255),
    first_pause_operator VARCHAR(255),
    first_pause_reason VARCHAR(255),
    first_resume_operator VARCHAR(255),
    first_resume_time VARCHAR(255),

    second_pause_time VARCHAR(255),
    second_pause_operator VARCHAR(255),
    second_pause_reason VARCHAR(255),
    second_resume_operator VARCHAR(255),
    second_resume_time VARCHAR(255),

    third_pause_time VARCHAR(255),
    third_pause_operator VARCHAR(255),
    third_pause_reason VARCHAR(255),
    third_resume_operator VARCHAR(255),
    third_resume_time VARCHAR(255),

    fourth_pause_time VARCHAR(255),
    fourth_pause_operator VARCHAR(255),
    fourth_pause_reason VARCHAR(255),
    fourth_resume_operator VARCHAR(255),
    fourth_resume_time VARCHAR(255),

    fifth_pause_time VARCHAR(255),
    fifth_pause_operator VARCHAR(255),
    fifth_pause_reason VARCHAR(255),
    fifth_resume_operator VARCHAR(255),
    fifth_resume_time VARCHAR(255),

    -- Información adicional
    fuel_type VARCHAR(255),
    cancel_operator_name VARCHAR(255),

    -- Metadatos de ingesta
    fecha_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archivo_origen VARCHAR(255)
);

-- Crear índice en task_id para mejorar performance
CREATE INDEX idx_raw_gde_tasks_task_id ON raw.gde_tasks(task_id);

-- ============================================================================
-- TABLA ODS: Datos procesados con tipos correctos
-- ============================================================================

DROP TABLE IF EXISTS ods.gde_tasks CASCADE;

CREATE TABLE ods.gde_tasks (
    task_id VARCHAR(50) PRIMARY KEY,
    remedy_id VARCHAR(50),

    -- Timestamps
    create_time TIMESTAMP,
    dispatch_time TIMESTAMP,
    accept_time TIMESTAMP,
    depart_time TIMESTAMP,
    arrive_time TIMESTAMP,
    complete_time TIMESTAMP,
    require_finish_time TIMESTAMP,
    first_complete_time TIMESTAMP,
    cancel_time TIMESTAMP,

    -- Información de cancelación
    cancel_operator VARCHAR(100),
    cancel_reason VARCHAR(100),
    cancel_operator_name VARCHAR(100),

    -- Estado y prioridad
    task_status VARCHAR(20),
    site_priority VARCHAR(10),
    sla_status VARCHAR(20),
    suspend_state VARCHAR(20),

    -- Información del sitio
    site_id VARCHAR(20),
    site_id_name VARCHAR(100),
    site_location_type VARCHAR(20),
    title TEXT,
    description TEXT,

    -- Información geográfica
    zona VARCHAR(20),
    departamento VARCHAR(50),
    region VARCHAR(50),

    -- Información de la tarea
    task_type VARCHAR(10),
    task_category VARCHAR(50),
    task_subcategory VARCHAR(50),
    nro_toa VARCHAR(20),

    -- SLA
    sla INTEGER,

    -- Operadores
    create_operator VARCHAR(100),
    create_operator_name VARCHAR(100),
    schedule_operator VARCHAR(100),
    schedule_operator_name VARCHAR(100),
    schedule_time TIMESTAMP,
    assign_to_fme VARCHAR(100),
    assign_to_fme_full_name VARCHAR(100),
    dispatch_operator VARCHAR(100),
    dispatch_operator_name VARCHAR(100),
    depart_operator VARCHAR(100),
    depart_operator_name VARCHAR(100),
    arrive_operator VARCHAR(100),
    arrive_operator_name VARCHAR(100),
    complete_operator VARCHAR(100),
    complete_operator_name VARCHAR(100),

    -- Información de rechazo
    reject_time TIMESTAMP,
    reject_operator VARCHAR(100),
    reject_operator_name VARCHAR(100),
    reject_des VARCHAR(100),
    reject_flag VARCHAR(20),
    reject_counter INTEGER,

    -- Información de fallas
    com_fault_speciality VARCHAR(50),
    com_fault_sub_speciality VARCHAR(100),
    com_fault_cause VARCHAR(100),
    fault_first_occur_time TIMESTAMP,
    fault_type VARCHAR(50),

    -- Equipos afectados
    com_level_1_aff_equip VARCHAR(50),
    com_level_2_aff_equip VARCHAR(50),
    com_level_3_aff_equip VARCHAR(50),

    -- Observaciones
    leave_observations TEXT,
    situacion_encontrada TEXT,
    detalle_actuacion_realizada TEXT,

    -- Información de contratistas
    fme_contractor VARCHAR(50),
    contratista_sitio VARCHAR(50),
    company_supply VARCHAR(50),
    torrero VARCHAR(50),
    fm_office VARCHAR(100),

    -- Información de incidencias
    atencion_incidencias VARCHAR(20),

    -- Pausas y reanudaciones
    first_pause_time TIMESTAMP,
    first_pause_operator VARCHAR(100),
    first_pause_reason VARCHAR(100),
    first_resume_operator VARCHAR(100),
    first_resume_time TIMESTAMP,

    second_pause_time TIMESTAMP,
    second_pause_operator VARCHAR(100),
    second_pause_reason VARCHAR(100),
    second_resume_operator VARCHAR(100),
    second_resume_time TIMESTAMP,

    third_pause_time TIMESTAMP,
    third_pause_operator VARCHAR(100),
    third_pause_reason VARCHAR(100),
    third_resume_operator VARCHAR(100),
    third_resume_time TIMESTAMP,

    fourth_pause_time TIMESTAMP,
    fourth_pause_operator VARCHAR(100),
    fourth_pause_reason VARCHAR(100),
    fourth_resume_operator VARCHAR(100),
    fourth_resume_time TIMESTAMP,

    fifth_pause_time TIMESTAMP,
    fifth_pause_operator VARCHAR(100),
    fifth_pause_reason VARCHAR(100),
    fifth_resume_operator VARCHAR(100),
    fifth_resume_time TIMESTAMP,

    -- Información adicional
    fuel_type VARCHAR(50),

    -- Metadatos de procesamiento
    fecha_procesamiento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_ingesta_raw TIMESTAMP
);

-- Crear índices para mejorar consultas
CREATE INDEX idx_ods_gde_tasks_status ON ods.gde_tasks(task_status);
CREATE INDEX idx_ods_gde_tasks_type ON ods.gde_tasks(task_type);
CREATE INDEX idx_ods_gde_tasks_create_time ON ods.gde_tasks(create_time);
CREATE INDEX idx_ods_gde_tasks_site_id ON ods.gde_tasks(site_id);

-- ============================================================================
-- TABLA DE LOGS PARA STORED PROCEDURES
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.sp_execution_log (
    log_id SERIAL PRIMARY KEY,
    sp_name VARCHAR(255) NOT NULL,
    estado VARCHAR(50) NOT NULL,
    msj_error TEXT,
    registros_procesados INTEGER,
    fecha_ejecucion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- FUNCIÓN AUXILIAR: Convertir VARCHAR a TIMESTAMP de forma segura
-- ============================================================================

CREATE OR REPLACE FUNCTION public.safe_to_timestamp(text_value TEXT)
RETURNS TIMESTAMP AS $$
BEGIN
    -- Intenta convertir el texto a timestamp
    -- Si falla, retorna NULL en lugar de error
    RETURN text_value::TIMESTAMP;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- FUNCIÓN AUXILIAR: Convertir VARCHAR a INTEGER de forma segura
-- ============================================================================

CREATE OR REPLACE FUNCTION public.safe_to_integer(text_value TEXT)
RETURNS INTEGER AS $$
BEGIN
    -- Intenta convertir el texto a integer
    -- Si falla, retorna NULL en lugar de error
    RETURN text_value::INTEGER;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- FUNCIÓN PARA OBTENER ÚLTIMO LOG DE SP
-- ============================================================================

CREATE OR REPLACE FUNCTION public.log_sp_ultimo_fn(sp_name_param TEXT)
RETURNS TABLE(
    sp_name VARCHAR(255),
    estado VARCHAR(50),
    msj_error TEXT,
    registros_procesados INTEGER,
    fecha_ejecucion TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        l.sp_name,
        l.estado,
        l.msj_error,
        l.registros_procesados,
        l.fecha_ejecucion
    FROM public.sp_execution_log l
    WHERE l.sp_name = sp_name_param
    ORDER BY l.fecha_ejecucion DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- STORED PROCEDURE PRINCIPAL: sp_cargar_gde_tasks
-- Transforma datos de raw.gde_tasks → ods.gde_tasks
-- ============================================================================

CREATE OR REPLACE PROCEDURE ods.sp_cargar_gde_tasks()
LANGUAGE plpgsql
AS $$
DECLARE
    v_registros_procesados INTEGER := 0;
    v_error_msg TEXT;
BEGIN
    -- Log inicio del proceso
    RAISE NOTICE 'Iniciando transformación de GDE tasks (RAW → ODS)...';

    -- Limpiar tabla ODS (modo REPLACE)
    TRUNCATE TABLE ods.gde_tasks;
    RAISE NOTICE 'Tabla ODS limpiada';

    -- Insertar datos transformados de RAW a ODS
    INSERT INTO ods.gde_tasks (
        task_id,
        remedy_id,
        create_time,
        dispatch_time,
        accept_time,
        depart_time,
        arrive_time,
        complete_time,
        require_finish_time,
        first_complete_time,
        cancel_time,
        cancel_operator,
        cancel_reason,
        cancel_operator_name,
        task_status,
        site_priority,
        sla_status,
        suspend_state,
        site_id,
        site_id_name,
        site_location_type,
        title,
        description,
        zona,
        departamento,
        region,
        task_type,
        task_category,
        task_subcategory,
        nro_toa,
        sla,
        create_operator,
        create_operator_name,
        schedule_operator,
        schedule_operator_name,
        schedule_time,
        assign_to_fme,
        assign_to_fme_full_name,
        dispatch_operator,
        dispatch_operator_name,
        depart_operator,
        depart_operator_name,
        arrive_operator,
        arrive_operator_name,
        complete_operator,
        complete_operator_name,
        reject_time,
        reject_operator,
        reject_operator_name,
        reject_des,
        reject_flag,
        reject_counter,
        com_fault_speciality,
        com_fault_sub_speciality,
        com_fault_cause,
        fault_first_occur_time,
        fault_type,
        com_level_1_aff_equip,
        com_level_2_aff_equip,
        com_level_3_aff_equip,
        leave_observations,
        situacion_encontrada,
        detalle_actuacion_realizada,
        fme_contractor,
        contratista_sitio,
        company_supply,
        torrero,
        fm_office,
        atencion_incidencias,
        first_pause_time,
        first_pause_operator,
        first_pause_reason,
        first_resume_operator,
        first_resume_time,
        second_pause_time,
        second_pause_operator,
        second_pause_reason,
        second_resume_operator,
        second_resume_time,
        third_pause_time,
        third_pause_operator,
        third_pause_reason,
        third_resume_operator,
        third_resume_time,
        fourth_pause_time,
        fourth_pause_operator,
        fourth_pause_reason,
        fourth_resume_operator,
        fourth_resume_time,
        fifth_pause_time,
        fifth_pause_operator,
        fifth_pause_reason,
        fifth_resume_operator,
        fifth_resume_time,
        fuel_type,
        fecha_ingesta_raw
    )
    SELECT
        TRIM(task_id),
        TRIM(remedy_id),
        -- Conversión segura de timestamps
        public.safe_to_timestamp(create_time),
        public.safe_to_timestamp(dispatch_time),
        public.safe_to_timestamp(accept_time),
        public.safe_to_timestamp(depart_time),
        public.safe_to_timestamp(arrive_time),
        public.safe_to_timestamp(complete_time),
        public.safe_to_timestamp(require_finish_time),
        public.safe_to_timestamp(first_complete_time),
        public.safe_to_timestamp(cancel_time),
        TRIM(cancel_operator),
        TRIM(cancel_reason),
        TRIM(cancel_operator_name),
        TRIM(task_status),
        TRIM(site_priority),
        TRIM(sla_status),
        TRIM(suspend_state),
        TRIM(site_id),
        TRIM(site_id_name),
        TRIM(site_location_type),
        title,
        description,
        TRIM(zona),
        TRIM(departamento),
        TRIM(region),
        TRIM(task_type),
        TRIM(task_category),
        TRIM(task_subcategory),
        TRIM(nro_toa),
        -- Conversión segura de enteros
        public.safe_to_integer(sla),
        TRIM(create_operator),
        TRIM(create_operator_name),
        TRIM(schedule_operator),
        TRIM(schedule_operator_name),
        public.safe_to_timestamp(schedule_time),
        TRIM(assign_to_fme),
        TRIM(assign_to_fme_full_name),
        TRIM(dispatch_operator),
        TRIM(dispatch_operator_name),
        TRIM(depart_operator),
        TRIM(depart_operator_name),
        TRIM(arrive_operator),
        TRIM(arrive_operator_name),
        TRIM(complete_operator),
        TRIM(complete_operator_name),
        public.safe_to_timestamp(reject_time),
        TRIM(reject_operator),
        TRIM(reject_operator_name),
        TRIM(reject_des),
        TRIM(reject_flag),
        public.safe_to_integer(reject_counter),
        TRIM(com_fault_speciality),
        TRIM(com_fault_sub_speciality),
        TRIM(com_fault_cause),
        public.safe_to_timestamp(fault_first_occur_time),
        TRIM(fault_type),
        TRIM(com_level_1_aff_equip),
        TRIM(com_level_2_aff_equip),
        TRIM(com_level_3_aff_equip),
        leave_observations,
        situacion_encontrada,
        detalle_actuacion_realizada,
        TRIM(fme_contractor),
        TRIM(contratista_sitio),
        TRIM(company_supply),
        TRIM(torrero),
        TRIM(fm_office),
        TRIM(atencion_incidencias),
        public.safe_to_timestamp(first_pause_time),
        TRIM(first_pause_operator),
        TRIM(first_pause_reason),
        TRIM(first_resume_operator),
        public.safe_to_timestamp(first_resume_time),
        public.safe_to_timestamp(second_pause_time),
        TRIM(second_pause_operator),
        TRIM(second_pause_reason),
        TRIM(second_resume_operator),
        public.safe_to_timestamp(second_resume_time),
        public.safe_to_timestamp(third_pause_time),
        TRIM(third_pause_operator),
        TRIM(third_pause_reason),
        TRIM(third_resume_operator),
        public.safe_to_timestamp(third_resume_time),
        public.safe_to_timestamp(fourth_pause_time),
        TRIM(fourth_pause_operator),
        TRIM(fourth_pause_reason),
        TRIM(fourth_resume_operator),
        public.safe_to_timestamp(fourth_resume_time),
        public.safe_to_timestamp(fifth_pause_time),
        TRIM(fifth_pause_operator),
        TRIM(fifth_pause_reason),
        TRIM(fifth_resume_operator),
        public.safe_to_timestamp(fifth_resume_time),
        TRIM(fuel_type),
        fecha_ingesta
    FROM raw.gde_tasks
    WHERE task_id IS NOT NULL AND TRIM(task_id) != '';

    -- Obtener cantidad de registros procesados
    GET DIAGNOSTICS v_registros_procesados = ROW_COUNT;

    -- Registrar éxito en log
    INSERT INTO public.sp_execution_log (sp_name, estado, msj_error, registros_procesados)
    VALUES ('ods.sp_cargar_gde_tasks()', 'success', NULL, v_registros_procesados);

    RAISE NOTICE 'Transformación completada: % registros procesados', v_registros_procesados;

EXCEPTION WHEN OTHERS THEN
    -- Capturar error y registrarlo
    GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;

    INSERT INTO public.sp_execution_log (sp_name, estado, msj_error, registros_procesados)
    VALUES ('ods.sp_cargar_gde_tasks()', 'error', v_error_msg, 0);

    RAISE NOTICE 'Error en transformación: %', v_error_msg;
    RAISE;
END;
$$;

-- ============================================================================
-- COMENTARIOS EN LAS TABLAS
-- ============================================================================

COMMENT ON TABLE raw.gde_tasks IS 'Tabla RAW: Datos crudos de GDE exportados desde Excel (todos VARCHAR)';
COMMENT ON TABLE ods.gde_tasks IS 'Tabla ODS: Datos procesados de GDE con tipos correctos y validaciones';
COMMENT ON TABLE public.sp_execution_log IS 'Log de ejecución de stored procedures';

-- ============================================================================
-- PERMISOS (ajustar según tu configuración)
-- ============================================================================

-- GRANT SELECT, INSERT, UPDATE, DELETE ON raw.gde_tasks TO etl_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ods.gde_tasks TO etl_user;
-- GRANT EXECUTE ON PROCEDURE ods.sp_cargar_gde_tasks() TO etl_user;

-- ============================================================================
-- FIN DEL SCRIPT
-- ============================================================================
