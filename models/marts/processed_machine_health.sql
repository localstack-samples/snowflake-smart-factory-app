{{
    config(
        materialized='table',
        schema='marts',
        alias='machine_health_metrics',
        pre_hook="DROP TABLE IF EXISTS {{ this }}"
    )
}}

WITH sensor_data AS (
    SELECT * FROM {{ ref('sensor_readings_view') }}
),

-- Basic metrics without complex calculations
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
    GROUP BY 1
),

-- Simplified health assessment
health_assessment AS (
    SELECT
        machine_id,
        last_reading_time,
        total_readings,
        anomalous_readings,
        avg_temperature,
        max_vibration,
        min_signal_strength,
        -- Calculate failure risk score (0-100)
        CASE
            WHEN avg_temperature > 90 OR max_vibration > 0.8 THEN 90 -- High risk
            WHEN avg_temperature > 75 OR max_vibration > 0.6 THEN 60 -- Medium risk
            ELSE 30 -- Low risk
        END as failure_risk_score,
        CASE
            WHEN avg_temperature > 90 OR max_vibration > 0.8 THEN 'CRITICAL'
            WHEN avg_temperature > 75 OR max_vibration > 0.6 THEN 'NEEDS_MAINTENANCE'
            ELSE 'HEALTHY'
        END as health_status,
        CASE
            WHEN avg_temperature > 90 OR max_vibration > 0.8 THEN 'Immediate maintenance required'
            WHEN avg_temperature > 75 OR max_vibration > 0.6 THEN 'Schedule maintenance within 48 hours'
            ELSE 'No action needed'
        END as maintenance_recommendation
    FROM machine_metrics
)

SELECT * FROM health_assessment