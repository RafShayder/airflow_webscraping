-- =====================================================
-- Script de inicialización de la base de datos
-- Se ejecuta automáticamente al crear el contenedor
-- =====================================================

-- Crear esquema raw
CREATE SCHEMA IF NOT EXISTS raw;

-- Crear tabla GDE (si existe el archivo)
-- \i /docker-entrypoint-initdb.d/create_gde_table.sql

-- Crear tabla Dynamic Checklist (si existe el archivo)
-- \i /docker-entrypoint-initdb.d/create_dynamic_checklist_table.sql

-- Crear usuario con permisos
GRANT ALL PRIVILEGES ON DATABASE scraper_db TO scraper_user;
GRANT ALL PRIVILEGES ON SCHEMA raw TO scraper_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO scraper_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO scraper_user;

-- Configurar permisos por defecto
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO scraper_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON SEQUENCES TO scraper_user;
