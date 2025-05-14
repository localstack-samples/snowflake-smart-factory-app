-- models/staging/sensor_readings_view.sql
{{
    config(
        materialized='view',
        schema='staging',
        alias='sensor_readings_view'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_sensor_data') }}
),

validated AS (
    SELECT
        machine_id,
        timestamp as event_time,
        -- Validate temperature readings
        CASE 
            WHEN temperature < 0 OR temperature > 150 THEN NULL  -- Invalid temperature range
            ELSE temperature
        END AS avg_temperature,
        
        -- Validate vibration readings
        CASE 
            WHEN vibration < 0 OR vibration > 2.0 THEN NULL  -- Invalid vibration range
            ELSE vibration
        END AS max_vibration,
        
        -- Convert status code to signal strength
        CASE
            WHEN status_code = 'AOK' THEN 100
            WHEN status_code = 'WARN' THEN 60
            WHEN status_code = 'CRIT' THEN 20
            ELSE 0
        END AS signal_strength,
        
        -- Flag potentially anomalous readings
        CASE 
            WHEN temperature > {{ var('sensor_reading_threshold') }} 
                OR vibration > 1.0 
                OR status_code = 'CRIT' THEN TRUE
            ELSE FALSE
        END AS is_anomalous
    FROM source
),

final AS (
    SELECT 
        *,
        -- Add reading status based on validation
        CASE 
            WHEN avg_temperature IS NULL OR max_vibration IS NULL THEN 'invalid'
            WHEN is_anomalous THEN 'anomalous'
            ELSE 'normal'
        END AS reading_status
    FROM validated
    WHERE avg_temperature IS NOT NULL 
        AND max_vibration IS NOT NULL  -- Filter out invalid readings
)

SELECT * FROM final 