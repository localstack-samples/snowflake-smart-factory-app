{{
  config(
    materialized          = 'incremental',
    schema                = 'raw',
    alias                 = 'sensor_data',
    unique_key            = 'event_time',
    incremental_strategy  = 'append',
    on_schema_change      = 'sync_all_columns'
  )
}}

{% if is_incremental() %}

WITH source AS (
    SELECT
        machine_id,
        "timestamp"::timestamp_ntz AS event_time,
        temperature,
        vibration,
        pressure,
        status_code
    FROM {{ source('factory', 'raw_sensor_data') }}
),

bounds AS (
    SELECT
        COALESCE(
            MAX(event_time),
            TO_TIMESTAMP_LTZ('1900-01-01T00:00:00Z')
        ) AS last_time
    FROM {{ this }}
)

SELECT
    s.machine_id,
    s.event_time,
    s.temperature,
    s.vibration,
    s.pressure,
    s.status_code,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source s
CROSS JOIN bounds b
WHERE s.event_time > b.last_time

{% else %}

SELECT
    machine_id,
    "timestamp"::timestamp_ntz AS event_time,
    temperature,
    vibration,
    pressure,
    status_code,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ source('factory', 'raw_sensor_data') }}

{% endif %}
