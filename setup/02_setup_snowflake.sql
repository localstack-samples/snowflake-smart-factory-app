-- Create and use database
CREATE DATABASE IF NOT EXISTS FACTORY_PIPELINE_DEMO;
USE DATABASE FACTORY_PIPELINE_DEMO;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- Raw Sensor Data Table (only table we need)
CREATE OR REPLACE TABLE RAW_SENSOR_DATA (
    machine_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ,
    temperature FLOAT,
    vibration FLOAT,
    pressure FLOAT,
    status_code VARCHAR(10)
);

-- Create a file format for CSV files
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Create stage pointing to S3 bucket
CREATE OR REPLACE STAGE sensor_data_stage
    URL = 's3://factory-sensor-data-local/raw_data/'
    CREDENTIALS = (AWS_KEY_ID='test' AWS_SECRET_KEY='test')
    FILE_FORMAT = csv_format
    AWS_ROLE = NULL;

-- Copy data from stage into RAW_SENSOR_DATA table
COPY INTO RAW_SENSOR_DATA
    FROM @sensor_data_stage
    PATTERN='.*sensor_data.*[.]csv'
    ON_ERROR = 'CONTINUE';

-- Validate data load
SELECT COUNT(*) as total_records FROM RAW_SENSOR_DATA;

-- Validate data quality
SELECT 
    'Data Quality Check' as check_type,
    COUNT(*) as total_records,
    COUNT(DISTINCT machine_id) as unique_machines,
    COUNT(*) - COUNT(temperature) as null_temperatures,
    COUNT(*) - COUNT(vibration) as null_vibrations,
    COUNT(*) - COUNT(status_code) as null_status_codes,
    MIN(timestamp) as earliest_reading,
    MAX(timestamp) as latest_reading
FROM RAW_SENSOR_DATA;

-- Sample data preview
SELECT * FROM RAW_SENSOR_DATA LIMIT 5;

-- Note: All transformations are now handled by dbt models:
-- - Staging: models/staging/stg_sensor_readings.sql
-- - Marts: models/marts/processed_machine_health.sql
