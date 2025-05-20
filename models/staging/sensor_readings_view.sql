-- models/staging/sensor_readings_view.sql

{{
  config(
    materialized = 'view',
    schema       = 'staging',
    alias        = 'sensor_readings_view'
  )
}}

WITH source AS (
    SELECT *
    FROM {{ ref('raw_sensor_data') }}
),

base_readings AS (
    SELECT
        machine_id,
        event_time,
        temperature,
        vibration,
        pressure,
        status_code
    FROM source
),

temperature_validation AS (
    SELECT
        *,
        CASE
            WHEN temperature < 0 OR temperature > 150 THEN NULL
            ELSE temperature
        END AS avg_temperature
    FROM base_readings
),

vibration_validation AS (
    SELECT
        *,
        CASE
            WHEN vibration < 0 OR vibration > 2.0 THEN NULL
            ELSE vibration
        END AS max_vibration
    FROM temperature_validation
),

pressure_validation AS (
    SELECT
        *,
        CASE
            WHEN pressure < 0 OR pressure > 500 THEN NULL
            ELSE pressure
        END AS validated_pressure
    FROM vibration_validation
),

signal_strength_calc AS (
    SELECT
        *,
        CASE
            WHEN status_code = 'AOK' THEN 100
            WHEN status_code = 'WARN' THEN 60
            WHEN status_code = 'CRIT' THEN 20
            ELSE 0
        END AS signal_strength
    FROM pressure_validation
),

anomaly_detection AS (
    SELECT
        *,
        CASE
            WHEN avg_temperature > {{ var('sensor_reading_threshold') }}
                OR max_vibration > 1.0
                OR validated_pressure > 450
                OR status_code = 'CRIT' THEN TRUE
            ELSE FALSE
        END AS is_anomalous
    FROM signal_strength_calc
)

SELECT
    machine_id,
    event_time,
    avg_temperature,
    max_vibration,
    validated_pressure  AS pressure,
    signal_strength,
    is_anomalous,
    CASE
        WHEN avg_temperature IS NULL
            OR max_vibration IS NULL
            OR validated_pressure IS NULL THEN 'invalid'
        WHEN is_anomalous THEN 'anomalous'
        ELSE 'normal'
    END AS reading_status
FROM anomaly_detection
WHERE avg_temperature IS NOT NULL
  AND max_vibration    IS NOT NULL
  AND validated_pressure IS NOT NULL
