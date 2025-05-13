-- Create and use database
CREATE DATABASE IF NOT EXISTS FACTORY_PIPELINE_DEMO;
USE DATABASE FACTORY_PIPELINE_DEMO;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- Raw Sensor Data Table
CREATE OR REPLACE TABLE RAW_SENSOR_DATA (
    machine_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ,
    temperature FLOAT,
    vibration FLOAT,
    pressure FLOAT,
    status_code VARCHAR(10)
);

CREATE OR REPLACE TABLE STG_SENSOR_READINGS (
    machine_id VARCHAR(50),
    event_time TIMESTAMP_NTZ,
    avg_temperature FLOAT,
    max_vibration FLOAT,
    signal_strength INTEGER
);

-- Processed Machine Health Table
CREATE OR REPLACE TABLE PROCESSED_MACHINE_HEALTH (
    machine_id VARCHAR(50),
    last_reading_time TIMESTAMP_NTZ,
    health_status VARCHAR(50),
    failure_risk_score FLOAT,
    maintenance_recommendation VARCHAR(255)
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

-- Verify data load
SELECT COUNT(*) AS raw_data_count FROM RAW_SENSOR_DATA;

-- Create view for machine health calculations
CREATE VIEW MACHINE_HEALTH_VIEW AS
SELECT 
    machine_id,
    timestamp as last_reading_time,
    CASE 
        WHEN temperature < 75 AND vibration < 0.5 THEN 'HEALTHY'
        WHEN temperature < 85 OR vibration < 0.7 THEN 'NEEDS_MAINTENANCE'
        ELSE 'CRITICAL'
    END as health_status,
    CASE 
        WHEN temperature < 75 AND vibration < 0.5 THEN 0.2
        WHEN temperature < 85 OR vibration < 0.7 THEN 0.6
        ELSE 0.9
    END as failure_risk_score,
    CASE 
        WHEN temperature < 75 AND vibration < 0.5 THEN 'No action needed'
        WHEN temperature < 85 OR vibration < 0.7 THEN 'Schedule maintenance within 48 hours'
        ELSE 'Immediate maintenance required'
    END as maintenance_recommendation
FROM RAW_SENSOR_DATA;

-- Insert data directly into PROCESSED_MACHINE_HEALTH
INSERT INTO PROCESSED_MACHINE_HEALTH
SELECT * FROM MACHINE_HEALTH_VIEW;

-- Final verification
SELECT 'RAW_SENSOR_DATA' as table_name, COUNT(*) as count FROM RAW_SENSOR_DATA
UNION ALL
SELECT 'PROCESSED_MACHINE_HEALTH' as table_name, COUNT(*) as count FROM PROCESSED_MACHINE_HEALTH;
SELECT * FROM PROCESSED_MACHINE_HEALTH;
