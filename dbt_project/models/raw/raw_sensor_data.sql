{{
    config(
        materialized='view',
        schema='raw',
        alias='sensor_data_view'
    )
}}

SELECT 
    machine_id,
    timestamp,
    temperature,
    vibration,
    pressure,
    status_code,
    CURRENT_TIMESTAMP() as _loaded_at
FROM {{ source('factory', 'raw_sensor_data') }} 