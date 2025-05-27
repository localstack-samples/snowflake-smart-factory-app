-- =====================================================
-- SNOWFLAKE ZERO-COPY CLONE DATA RECOVERY SOLUTION
-- =====================================================
-- This demo showcases Snowflake's Zero-Copy Cloning for instant
-- data backup and recovery in a smart factory environment.
 
-- Target Table: FACTORY_PIPELINE_DEMO.PUBLIC.RAW_SENSOR_DATA
-- Data Range: 2024-05-13 10:00:00 to 11:27:54 UTC
-- =====================================================

-- Set context
USE DATABASE FACTORY_PIPELINE_DEMO;
USE SCHEMA PUBLIC;

-- =====================================================
-- PART 1: SETUP - Create a backup clone BEFORE the demo
-- =====================================================
-- In a real scenario, this would be done proactively
-- For demo purposes, we'll create it just before the "oops" moment

-- Create a backup clone of the current data
CREATE OR REPLACE TABLE RAW_SENSOR_DATA_BACKUP 
CLONE RAW_SENSOR_DATA;

-- Verify the backup was created successfully
SELECT 'BACKUP CREATED' as status, COUNT(*) as record_count 
FROM RAW_SENSOR_DATA_BACKUP;

-- =====================================================
-- PART 2: BASELINE - Show current data state
-- =====================================================

-- Show current data summary
SELECT 
    'BEFORE OOPS' as checkpoint,
    COUNT(*) as total_records,
    COUNT(DISTINCT machine_id) as unique_machines,
    MIN(timestamp) as earliest_reading,
    MAX(timestamp) as latest_reading,
    AVG(temperature) as avg_temperature,
    COUNT(CASE WHEN status_code = 'CRIT' THEN 1 END) as critical_alerts
FROM RAW_SENSOR_DATA;

-- Show sample of recent data for specific machines
SELECT 
    machine_id, 
    timestamp, 
    temperature, 
    vibration, 
    pressure, 
    status_code
FROM RAW_SENSOR_DATA 
WHERE machine_id IN ('M001', 'M002')
ORDER BY timestamp DESC 
LIMIT 10;

-- =====================================================
-- PART 3: THE "OOPS" MOMENT - Simulate accidental data corruption
-- =====================================================

-- Delete recent sensor readings for M001 and M002 (simulates accidental deletion)
-- Delete data from the last hour of operations (11:00 onwards)
DELETE FROM RAW_SENSOR_DATA 
WHERE machine_id IN ('M001', 'M002') 
  AND timestamp >= '2024-05-13T11:00:00Z';

-- =====================================================
-- PART 4: SHOW THE DAMAGE
-- =====================================================

-- Let's see the impact of our "oops" moment
SELECT 
    'AFTER OOPS' as checkpoint,
    COUNT(*) as total_records,
    COUNT(DISTINCT machine_id) as unique_machines,
    MIN(timestamp) as earliest_reading,
    MAX(timestamp) as latest_reading,
    AVG(temperature) as avg_temperature,
    COUNT(CASE WHEN status_code = 'CRIT' THEN 1 END) as critical_alerts
FROM RAW_SENSOR_DATA;

-- M001 and M002 recent data is missing
SELECT 
    'MISSING DATA CHECK' as status,
    machine_id,
    COUNT(*) as remaining_records,
    MAX(timestamp) as latest_timestamp
FROM RAW_SENSOR_DATA 
WHERE machine_id IN ('M001', 'M002')
GROUP BY machine_id
ORDER BY machine_id;

-- Verify no data exists after 11:00 for M001 and M002
SELECT 
    machine_id, 
    timestamp, 
    temperature, 
    status_code
FROM RAW_SENSOR_DATA 
WHERE machine_id IN ('M001', 'M002')
  AND timestamp >= '2024-05-13T11:00:00Z'
ORDER BY timestamp DESC;

-- =====================================================
-- PART 5: INSTANT RECOVERY - ZERO-COPY CLONE
-- =====================================================

-- Show what data exists in our backup clone
SELECT 
    'BACKUP CLONE DATA' as source,
    COUNT(*) as total_records,
    COUNT(DISTINCT machine_id) as unique_machines,
    COUNT(CASE WHEN machine_id IN ('M001', 'M002') THEN 1 END) as m001_m002_records
FROM RAW_SENSOR_DATA_BACKUP;

-- Show the missing data still exists in our backup
SELECT 
    'RECOVERED FROM BACKUP' as source,
    machine_id, 
    timestamp, 
    temperature, 
    vibration, 
    pressure, 
    status_code
FROM RAW_SENSOR_DATA_BACKUP
WHERE machine_id IN ('M001', 'M002') 
  AND timestamp >= '2024-05-13T11:00:00Z'
ORDER BY timestamp DESC 
LIMIT 10;

-- INSTANT RECOVERY using Zero-Copy Clone
-- Replace the damaged table with our backup clone
CREATE OR REPLACE TABLE RAW_SENSOR_DATA 
CLONE RAW_SENSOR_DATA_BACKUP;

-- Verify recovery
SELECT 
    'AFTER CLONE RECOVERY' as checkpoint,
    COUNT(*) as total_records,
    COUNT(DISTINCT machine_id) as unique_machines,
    COUNT(CASE WHEN machine_id IN ('M001', 'M002') THEN 1 END) as m001_m002_records
FROM RAW_SENSOR_DATA;

-- Confirm M001 and M002 data is restored
SELECT 
    'RESTORED DATA CHECK' as status,
    machine_id,
    COUNT(*) as total_records,
    MAX(timestamp) as latest_timestamp
FROM RAW_SENSOR_DATA 
WHERE machine_id IN ('M001', 'M002')
GROUP BY machine_id
ORDER BY machine_id;

-- =====================================================
-- PART 6: CLEANUP AND FINAL VERIFICATION
-- =====================================================

-- Final data quality check
SELECT 
    'FINAL STATE' as checkpoint,
    COUNT(*) as total_records,
    COUNT(DISTINCT machine_id) as unique_machines,
    MIN(timestamp) as earliest_reading,
    MAX(timestamp) as latest_reading,
    AVG(temperature) as avg_temperature,
    COUNT(CASE WHEN status_code = 'CRIT' THEN 1 END) as critical_alerts,
    COUNT(CASE WHEN temperature > 200 THEN 1 END) as corrupted_temps
FROM RAW_SENSOR_DATA;

-- Show that all machines have data across the full time range
SELECT 
    machine_id,
    COUNT(*) as record_count,
    MIN(timestamp) as earliest_reading,
    MAX(timestamp) as latest_reading,
    AVG(temperature) as avg_temp
FROM RAW_SENSOR_DATA 
GROUP BY machine_id 
ORDER BY machine_id;

-- Optional: Clean up demo artifacts
-- DROP TABLE IF EXISTS RAW_SENSOR_DATA_BACKUP;
-- DROP TABLE IF EXISTS RAW_SENSOR_DATA_BACKUP_20250115;
