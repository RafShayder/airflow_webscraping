CREATE TABLE raw.dynamic_checklist_tasks (
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
    
    -- Pausas y reanudaciones (hasta 5 niveles)
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
    
    -- Metadatos de ingesta
    fecha_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archivo_origen VARCHAR(255)
);
