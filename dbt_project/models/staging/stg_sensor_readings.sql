-- models/staging/stg_sensor_readings.sql
WITH raw_data AS (
    -- If using dbt sources.yml: SELECT * FROM {{ source('factory_raw', 'raw_sensor_data') }}
    -- For direct table reference, including database and schema:
    SELECT * FROM {{ var('db_name', 'FACTORY_PIPELINE_DEMO') }}.PUBLIC.RAW_SENSOR_DATA
)
SELECT
    machine_id,
    timestamp AS event_time,
    temperature AS avg_temperature, -- Keeping it simple, could be AVG() over a window for true staging
    vibration AS max_vibration,     -- Could be MAX() over a window for true staging
    CASE
        WHEN status_code = 'AOK' THEN 100
        WHEN status_code = 'WARN' THEN 60
        WHEN status_code = 'CRIT' THEN 20
        ELSE 0
    END AS signal_strength, -- Example derived metric
    (pressure - 100) * 10 AS pressure_anomaly_score -- Example moderate transformation
FROM raw_data
WHERE temperature IS NOT NULL AND vibration IS NOT NULL -- Basic data quality filter
