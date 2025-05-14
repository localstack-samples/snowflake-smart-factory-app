{{
    config(
        materialized='incremental',
        schema='raw',
        alias='sensor_data',
        unique_key='timestamp',
        incremental_strategy='timestamp',
        on_schema_change='sync_all_columns'
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
{% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
{% endif %}
