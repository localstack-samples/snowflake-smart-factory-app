{{
    config(
        materialized='table',
        schema='marts',
        alias='machine_health_metrics'
    )
}}

WITH sensor_data AS (
    SELECT * FROM {{ ref('stg_sensor_readings') }}
),

-- Calculate metrics per machine over the configured window
machine_metrics AS (
    SELECT
        machine_id,
        COUNT(*) as total_readings,
        COUNT(CASE WHEN reading_status = 'anomalous' THEN 1 END) as anomalous_readings,
        AVG(avg_temperature) as avg_temperature,
        MAX(max_vibration) as max_vibration,
        MIN(signal_strength) as min_signal_strength,
        MAX(event_time) as last_reading_time
    FROM sensor_data
    WHERE event_time >= DATEADD(hour, -{{ var('machine_health_window') }}, CURRENT_TIMESTAMP())
    GROUP BY 1
),

-- Calculate health scores
health_assessment AS (
    SELECT
        machine_id,
        last_reading_time,
        -- Temperature health score (0-100)
        CASE
            WHEN avg_temperature < 60 THEN 100
            WHEN avg_temperature < 75 THEN 80
            WHEN avg_temperature < 85 THEN 60
            WHEN avg_temperature < 95 THEN 40
            ELSE 20
        END as temperature_health_score,
        
        -- Vibration health score (0-100)
        CASE
            WHEN max_vibration < 0.3 THEN 100
            WHEN max_vibration < 0.5 THEN 80
            WHEN max_vibration < 0.7 THEN 60
            WHEN max_vibration < 0.9 THEN 40
            ELSE 20
        END as vibration_health_score,
        
        -- Signal strength as noise health score (0-100)
        min_signal_strength as noise_health_score,
        
        -- Pressure health score (placeholder since we don't have pressure data yet)
        80 as pressure_health_score,
        
        -- Calculate overall health score as weighted average
        (
            CASE
                WHEN avg_temperature < 60 THEN 100
                WHEN avg_temperature < 75 THEN 80
                WHEN avg_temperature < 85 THEN 60
                WHEN avg_temperature < 95 THEN 40
                ELSE 20
            END * 0.3 +
            CASE
                WHEN max_vibration < 0.3 THEN 100
                WHEN max_vibration < 0.5 THEN 80
                WHEN max_vibration < 0.7 THEN 60
                WHEN max_vibration < 0.9 THEN 40
                ELSE 20
            END * 0.3 +
            min_signal_strength * 0.2 +
            80 * 0.2
        ) as overall_health_score,
        
        -- Determine health status based on overall score
        CASE
            WHEN (
                CASE
                    WHEN avg_temperature < 60 THEN 100
                    WHEN avg_temperature < 75 THEN 80
                    WHEN avg_temperature < 85 THEN 60
                    WHEN avg_temperature < 95 THEN 40
                    ELSE 20
                END * 0.3 +
                CASE
                    WHEN max_vibration < 0.3 THEN 100
                    WHEN max_vibration < 0.5 THEN 80
                    WHEN max_vibration < 0.7 THEN 60
                    WHEN max_vibration < 0.9 THEN 40
                    ELSE 20
                END * 0.3 +
                min_signal_strength * 0.2 +
                80 * 0.2
            ) >= 80 THEN 'HEALTHY'
            WHEN (
                CASE
                    WHEN avg_temperature < 60 THEN 100
                    WHEN avg_temperature < 75 THEN 80
                    WHEN avg_temperature < 85 THEN 60
                    WHEN avg_temperature < 95 THEN 40
                    ELSE 20
                END * 0.3 +
                CASE
                    WHEN max_vibration < 0.3 THEN 100
                    WHEN max_vibration < 0.5 THEN 80
                    WHEN max_vibration < 0.7 THEN 60
                    WHEN max_vibration < 0.9 THEN 40
                    ELSE 20
                END * 0.3 +
                min_signal_strength * 0.2 +
                80 * 0.2
            ) >= 60 THEN 'NEEDS_MAINTENANCE'
            ELSE 'CRITICAL'
        END as health_status,
        
        -- Generate maintenance recommendation
        CASE
            WHEN (
                CASE
                    WHEN avg_temperature < 60 THEN 100
                    WHEN avg_temperature < 75 THEN 80
                    WHEN avg_temperature < 85 THEN 60
                    WHEN avg_temperature < 95 THEN 40
                    ELSE 20
                END * 0.3 +
                CASE
                    WHEN max_vibration < 0.3 THEN 100
                    WHEN max_vibration < 0.5 THEN 80
                    WHEN max_vibration < 0.7 THEN 60
                    WHEN max_vibration < 0.9 THEN 40
                    ELSE 20
                END * 0.3 +
                min_signal_strength * 0.2 +
                80 * 0.2
            ) >= 80 THEN 'No action needed'
            WHEN (
                CASE
                    WHEN avg_temperature < 60 THEN 100
                    WHEN avg_temperature < 75 THEN 80
                    WHEN avg_temperature < 85 THEN 60
                    WHEN avg_temperature < 95 THEN 40
                    ELSE 20
                END * 0.3 +
                CASE
                    WHEN max_vibration < 0.3 THEN 100
                    WHEN max_vibration < 0.5 THEN 80
                    WHEN max_vibration < 0.7 THEN 60
                    WHEN max_vibration < 0.9 THEN 40
                    ELSE 20
                END * 0.3 +
                min_signal_strength * 0.2 +
                80 * 0.2
            ) >= 60 THEN 'Schedule maintenance within 48 hours'
            ELSE 'Immediate maintenance required'
        END as maintenance_recommendation,
        
        -- Additional metrics
        total_readings,
        anomalous_readings,
        avg_temperature,
        max_vibration,
        min_signal_strength
    FROM machine_metrics
)

SELECT * FROM health_assessment 