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

-- Processed Machine Health Table (populated by Snowpark SP)
CREATE OR REPLACE TABLE PROCESSED_MACHINE_HEALTH (
    machine_id VARCHAR(50),
    last_reading_time TIMESTAMP_NTZ,
    health_status VARCHAR(50),
    failure_risk_score FLOAT,
    maintenance_recommendation VARCHAR(255)
);

-- S3 Stage (LocalStack S3)
-- For LocalStack, ensure your S3 endpoint is accessible and use dummy credentials.
-- The URL might need to be 's3://factory-sensor-data-local/raw_data/' if you have a prefix.
CREATE OR REPLACE STAGE local_s3_sensor_stage
    URL='s3://factory-sensor-data-local/'
    CREDENTIALS=(AWS_KEY_ID='test' AWS_SECRET_KEY='test');

-- Snowpipe for auto-ingestion
CREATE OR REPLACE PIPE sensor_data_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO RAW_SENSOR_DATA
    FROM @local_s3_sensor_stage/raw_data/
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE';

-- Show objects
SHOW STAGES;
SHOW PIPES;
SHOW TABLES;
