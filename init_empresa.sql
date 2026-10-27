-- ============================================================================
-- Universidad Loyola Andalucía - Base de Datos Empresarial de Movilidad
-- Asignatura: Ampliación de Bases de Datos
-- Esquema para la integración y explotación de analítica Spark SQL
-- ============================================================================

CREATE TABLE IF NOT EXISTS kpis_lineas_transporte (
    id SERIAL PRIMARY KEY,
    linea VARCHAR(60) NOT NULL,
    medio_transporte VARCHAR(50) NOT NULL,
    total_viajes INT NOT NULL,
    duracion_media_min NUMERIC(5,2) NOT NULL,
    retraso_medio_min NUMERIC(5,2) NOT NULL,
    ocupacion_media_pct NUMERIC(5,2) NOT NULL,
    co2_total_kg NUMERIC(10,2) NOT NULL,
    fecha_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kpis_franjas_horarias (
    id SERIAL PRIMARY KEY,
    franja VARCHAR(40) NOT NULL,
    hora_inicio INT NOT NULL,
    hora_fin INT NOT NULL,
    total_desplazamientos INT NOT NULL,
    pct_sobre_total NUMERIC(5,2) NOT NULL,
    medio_principal VARCHAR(50) NOT NULL,
    nivel_congestion VARCHAR(20) NOT NULL,
    fecha_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kpis_origenes_estudiantes (
    id SERIAL PRIMARY KEY,
    distrito_origen VARCHAR(80) NOT NULL,
    total_viajeros INT NOT NULL,
    medio_mas_usado VARCHAR(50) NOT NULL,
    tiempo_promedio_min NUMERIC(5,2) NOT NULL,
    distancia_media_km NUMERIC(5,2) NOT NULL,
    co2_evitado_kg NUMERIC(10,2) NOT NULL,
    fecha_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS registro_procesamiento_spark (
    id SERIAL PRIMARY KEY,
    fecha_ejecucion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    registros_analizados INT NOT NULL,
    tiempo_procesamiento_seg NUMERIC(6,3) NOT NULL,
    estado VARCHAR(30) NOT NULL,
    motor VARCHAR(60) DEFAULT 'Apache Spark 3.5.1 (Catalyst Engine)'
);
