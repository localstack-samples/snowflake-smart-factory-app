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
    -- 🐛 DEMO BUG: Incorrect CSV delimiter configuration
    -- The actual CSV file uses commas, not semicolons.
    -- This mismatch causes data parsing to fail in dbt test "table_not_empty"
    -- Snowpipe will fail to parse CSV data correctly and Streamlit app shows no data
    -- FIELD_DELIMITER = ';'  -- 🚨 BUG: Causes "RAW_SENSOR_DATA" table to remain empty
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

-- Create a Snowpipe for automated ingestion
CREATE OR REPLACE PIPE sensor_data_pipe 
  AUTO_INGEST = TRUE
  AS
  COPY INTO RAW_SENSOR_DATA
    FROM @sensor_data_stage
    PATTERN='.*[.]csv'
    ON_ERROR = 'CONTINUE';

-- Show pipe details (important to get notification_channel)
DESC PIPE sensor_data_pipe
