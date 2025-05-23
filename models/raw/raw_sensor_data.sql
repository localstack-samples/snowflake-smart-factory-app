{{
  config(
    materialized          = 'incremental',
    schema                = 'raw',
    alias                 = 'sensor_data',
    unique_key            = ['machine_id', 'event_time'],
    incremental_strategy  = 'merge',
    on_schema_change      = 'sync_all_columns'
  )
}}

{% if is_incremental() %}

WITH source AS (
    SELECT
        machine_id,
        timestamp::timestamp_ntz AS event_time,
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
),

filtered_source AS (
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
),

-- Deduplicate to ensure unique machine_id + event_time combinations
deduplicated AS (
    SELECT
        machine_id,
        event_time,
        temperature,
        vibration,
        pressure,
        status_code,
        _loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY machine_id, event_time 
            ORDER BY _loaded_at DESC
        ) as rn
    FROM filtered_source
)

SELECT
    machine_id,
    event_time,
    temperature,
    vibration,
    pressure,
    status_code,
    _loaded_at
FROM deduplicated
WHERE rn = 1

{% else %}

WITH source AS (
    SELECT
        machine_id,
        timestamp::timestamp_ntz AS event_time,
        temperature,
        vibration,
        pressure,
        status_code,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM {{ source('factory', 'raw_sensor_data') }}
),

-- Deduplicate initial load as well
deduplicated AS (
    SELECT
        machine_id,
        event_time,
        temperature,
        vibration,
        pressure,
        status_code,
        _loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY machine_id, event_time 
            ORDER BY _loaded_at DESC
        ) as rn
    FROM source
)

SELECT
    machine_id,
    event_time,
    temperature,
    vibration,
    pressure,
    status_code,
    _loaded_at
FROM deduplicated
WHERE rn = 1

{% endif %}
