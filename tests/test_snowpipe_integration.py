import pytest
import os
from datetime import datetime
import logging
from conftest import get_snowflake_connection

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_pipe_exists(snowflake_conn):
    """Test that the Snowpipe exists"""
    cursor = snowflake_conn.cursor()
    
    try:
        cursor.execute("DESC PIPE sensor_data_pipe")
        result = cursor.fetchall()
        assert len(result) > 0, "Snowpipe 'sensor_data_pipe' does not exist"
        logger.info("Verified Snowpipe 'sensor_data_pipe' exists")
    except Exception as e:
        logger.error(f"Error checking pipe existence: {e}")
        pytest.fail(f"Failed to check pipe existence: {e}")


def test_data_ingestion_count(snowflake_conn):
    """Test that data has been loaded via Snowpipe"""
    cursor = snowflake_conn.cursor()
    
    try:
        # Check total record count
        cursor.execute("SELECT COUNT(*) FROM RAW_SENSOR_DATA")
        count = cursor.fetchone()[0]
        
        # Based on sample data output showing 500 records
        assert count > 0, "No records found in RAW_SENSOR_DATA"
        logger.info(f"Found {count} records in RAW_SENSOR_DATA")
    except Exception as e:
        logger.error(f"Error checking data count: {e}")
        pytest.fail(f"Failed to check data count: {e}")


def test_data_quality(snowflake_conn):
    """Test data quality in RAW_SENSOR_DATA"""
    cursor = snowflake_conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                'Data Quality Check' as check_type,
                COUNT(*) as total_records,
                COUNT(DISTINCT machine_id) as unique_machines,
                COUNT(*) - COUNT(temperature) as null_temperatures,
                COUNT(*) - COUNT(vibration) as null_vibrations,
                COUNT(*) - COUNT(status_code) as null_status_codes,
                MIN(timestamp) as earliest_reading,
                MAX(timestamp) as latest_reading
            FROM RAW_SENSOR_DATA
        """)
        result = cursor.fetchone()
        
        # Extract results
        check_type, total_records, unique_machines, null_temperatures, \
        null_vibrations, null_status_codes, earliest_reading, latest_reading = result
        
        # Assertions based on expected values from output
        assert check_type == "Data Quality Check"
        assert total_records > 0, "No records found in the table"
        assert unique_machines == 10, f"Expected 10 unique machines, found {unique_machines}"
        assert null_temperatures == 0, f"Found {null_temperatures} null temperature values"
        assert null_vibrations == 0, f"Found {null_vibrations} null vibration values"
        assert null_status_codes == 0, f"Found {null_status_codes} null status code values"
        assert earliest_reading is not None, "No earliest reading timestamp found"
        assert latest_reading is not None, "No latest reading timestamp found"
        
        logger.info("Data quality check passed successfully")
        logger.info(f"Records: {total_records}, Machines: {unique_machines}, "
                   f"Date range: {earliest_reading} to {latest_reading}")
    except Exception as e:
        logger.error(f"Error checking data quality: {e}")
        pytest.fail(f"Failed to check data quality: {e}")


def test_data_sample_structure(snowflake_conn):
    """Test that data sample matches expected schema"""
    cursor = snowflake_conn.cursor()
    
    try:
        cursor.execute("SELECT * FROM RAW_SENSOR_DATA LIMIT 5")
        results = cursor.fetchall()
        
        # Verify we got rows back
        assert len(results) > 0, "No data found in RAW_SENSOR_DATA"
        
        # Verify schema/structure
        for row in results:
            assert len(row) == 6, f"Expected 6 columns, got {len(row)}"
            machine_id, timestamp, temperature, vibration, pressure, status_code = row
            
            # Type checking
            assert machine_id.startswith('M'), f"machine_id should start with 'M': {machine_id}"
            assert temperature is not None, "temperature is null"
            assert vibration is not None, "vibration is null"
            assert pressure is not None, "pressure is null"
            assert status_code in ["AOK", "WARN", "CRIT"], f"Unexpected status_code: {status_code}"
        
        logger.info("Data sample structure validated successfully")
    except Exception as e:
        logger.error(f"Error checking data sample: {e}")
        pytest.fail(f"Failed to check data sample: {e}")
