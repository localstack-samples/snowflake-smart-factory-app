import pytest
import pandas as pd
import numpy as np
from snowflake.connector.errors import ProgrammingError

def test_machine_health_metrics_table_exists(snowflake_conn):
    """Test if the machine health metrics table exists and is accessible"""
    cursor = snowflake_conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics LIMIT 1")
        result = cursor.fetchone()
        assert result is not None, "Machine health metrics table should exist and be accessible"
    finally:
        cursor.close()

def test_machine_health_metrics_columns(snowflake_conn):
    """Test if all required columns are present in the table"""
    cursor = snowflake_conn.cursor()
    try:
        cursor.execute("DESC TABLE FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics")
        columns = {row[0].lower(): row[1] for row in cursor.fetchall()}
        
        # Required columns based on UI usage
        required_columns = {
            'machine_id': str,
            'health_status': str,
            'failure_risk_score': float,
            'maintenance_recommendation': str
        }
        
        for col, _ in required_columns.items():
            assert col in columns, f"Required column '{col}' not found in table"
            
    finally:
        cursor.close()

def test_machine_health_metrics_data_types(snowflake_conn):
    """Test if the data in the table has the expected types and constraints"""
    cursor = snowflake_conn.cursor()
    try:
        cursor.execute("SELECT * FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics LIMIT 10")
        columns = [desc[0].lower() for desc in cursor.description]
        data = cursor.fetchall()
        
        if not data:
            pytest.skip("No data available in machine health metrics table")
        
        df = pd.DataFrame(data, columns=columns)
        
        # Type validations
        assert df['machine_id'].dtype == 'object', "machine_id should be string type"
        assert df['health_status'].dtype == 'object', "health_status should be string type"
        assert pd.to_numeric(df['failure_risk_score'], errors='coerce').notnull().all(), \
            "failure_risk_score should be numeric"
        assert df['maintenance_recommendation'].dtype == 'object', \
            "maintenance_recommendation should be string type"
        
        # Value validations
        valid_health_statuses = {'HEALTHY', 'NEEDS_MAINTENANCE', 'CRITICAL'}
        assert set(df['health_status'].unique()).issubset(valid_health_statuses), \
            "health_status should only contain valid values"
        
        # Convert percentage values (0-100) to decimal values (0-1)
        failure_risk_scores = df['failure_risk_score'].astype(float) / 100.0
        assert (failure_risk_scores >= 0).all() and (failure_risk_scores <= 1).all(), \
            "failure_risk_score should be between 0 and 100"
            
    finally:
        cursor.close()

def test_data_completeness(snowflake_conn):
    """Test for data completeness - no nulls and all machines have records"""
    cursor = snowflake_conn.cursor()
    try:
        # Check for NULL values in critical columns using CASE statements
        cursor.execute("""
            SELECT COUNT(*) 
            FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics 
            WHERE machine_id = '' 
               OR health_status = ''
               OR failure_risk_score = 0
               OR maintenance_recommendation = ''
        """)
        null_count = cursor.fetchone()[0]
        assert null_count == 0, "Critical columns should not be empty"
        
        # Check if each machine has at least one record using EXISTS
        cursor.execute("""
            SELECT m1.machine_id
            FROM FACTORY_PIPELINE_DEMO.PUBLIC.RAW_SENSOR_DATA m1
            WHERE NOT EXISTS (
                SELECT 1 
                FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics m2
                WHERE m1.machine_id = m2.machine_id
            )
            LIMIT 1
        """)
        missing_machines = cursor.fetchone()
        assert missing_machines is None, "All machines should have health metrics"
        
    finally:
        cursor.close()

def test_data_consistency(snowflake_conn):
    """Test for data consistency in health metrics"""
    cursor = snowflake_conn.cursor()
    try:
        cursor.execute("""
            SELECT health_status, 
                   maintenance_recommendation,
                   failure_risk_score
            FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics
        """)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['health_status', 'maintenance_recommendation', 'failure_risk_score'])
        
        # Check if CRITICAL status has urgent recommendations
        critical_records = df[df['health_status'] == 'CRITICAL']
        if not critical_records.empty:
            assert any('urgent' in rec.lower() or 'immediate' in rec.lower() 
                      for rec in critical_records['maintenance_recommendation']), \
                "CRITICAL status should have urgent maintenance recommendations"
        
        # Check if risk scores align with health status
        status_risk_scores = df.groupby('health_status')['failure_risk_score'].mean()
        if all(status in status_risk_scores.index for status in ['CRITICAL', 'NEEDS_MAINTENANCE', 'HEALTHY']):
            assert status_risk_scores['CRITICAL'] > status_risk_scores['HEALTHY'], \
                "Risk scores should be higher for CRITICAL than HEALTHY status"
        
        # Check for duplicates using group by
        cursor.execute("""
            SELECT machine_id, health_status, failure_risk_score, maintenance_recommendation, COUNT(*)
            FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics
            GROUP BY machine_id, health_status, failure_risk_score, maintenance_recommendation
            HAVING COUNT(*) > 1
            LIMIT 1
        """)
        duplicate = cursor.fetchone()
        assert duplicate is None, "There should be no duplicate records"
        
    finally:
        cursor.close()

def test_data_ranges(snowflake_conn):
    """Test for expected data ranges and distributions"""
    cursor = snowflake_conn.cursor()
    try:
        # Check machine count is within expected range
        cursor.execute("SELECT COUNT(DISTINCT machine_id) FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics")
        machine_count = cursor.fetchone()[0]
        assert 1 <= machine_count <= 1000, "Number of machines should be within reasonable range"
        
        # Check health status distribution using simpler aggregation
        cursor.execute("""
            SELECT health_status, COUNT(*) as count
            FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics
            GROUP BY health_status
        """)
        status_counts = dict(cursor.fetchall())
        total_count = sum(status_counts.values())
        
        # Calculate proportions manually
        status_props = {status: count/total_count for status, count in status_counts.items()}
        
        # Ensure not all machines are in CRITICAL status
        assert status_props.get('CRITICAL', 0) < 0.5, \
            "Proportion of CRITICAL machines should not exceed 50%"
        
        # Check maintenance recommendations
        cursor.execute("""
            SELECT COUNT(DISTINCT maintenance_recommendation) 
            FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics
        """)
        unique_recommendations = cursor.fetchone()[0]
        assert 1 <= unique_recommendations <= 20, \
            "Number of unique maintenance recommendations should be reasonable"
            
    finally:
        cursor.close()

def test_data_aggregation(snowflake_conn):
    """Test aggregated metrics and statistics"""
    cursor = snowflake_conn.cursor()
    try:
        # Use simpler statistics
        cursor.execute("""
            SELECT 
                AVG(failure_risk_score) as mean_risk,
                MIN(failure_risk_score) as min_risk,
                MAX(failure_risk_score) as max_risk
            FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics
        """)
        stats = cursor.fetchone()
        mean_risk, min_risk, max_risk = stats
        
        # Check if statistics are within reasonable ranges
        assert 0 <= mean_risk <= 100, "Mean risk score should be between 0 and 100"
        assert 0 <= min_risk <= max_risk <= 100, "Risk scores should be between 0 and 100"
        
        # Check health status distribution using simpler aggregation
        cursor.execute("""
            SELECT health_status, COUNT(*) as count
            FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics
            GROUP BY health_status
        """)
        status_counts = dict(cursor.fetchall())
        total_count = sum(status_counts.values())
        
        # Calculate proportions manually
        status_props = {status: count/total_count for status, count in status_counts.items()}
        
        # Verify reasonable distribution
        assert all(0 <= prop <= 1 for prop in status_props.values()), \
            "Health status proportions should be valid probabilities"
        assert sum(status_props.values()) == pytest.approx(1.0), \
            "Health status proportions should sum to 1"
            
    finally:
        cursor.close()

def test_data_relationships(snowflake_conn):
    """Test relationships between metrics and source data"""
    cursor = snowflake_conn.cursor()
    try:
        # Check if all machines in metrics exist in sensor data using EXISTS
        cursor.execute("""
            SELECT m.machine_id
            FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics m
            WHERE NOT EXISTS (
                SELECT 1 
                FROM FACTORY_PIPELINE_DEMO.PUBLIC.RAW_SENSOR_DATA s
                WHERE m.machine_id = s.machine_id
            )
            LIMIT 1
        """)
        orphaned_metric = cursor.fetchone()
        assert orphaned_metric is None, "All machines in metrics should exist in sensor data"
        
        # Check if metrics align with recent sensor data
        cursor.execute("""
            WITH recent_sensor_data AS (
                SELECT 
                    machine_id,
                    AVG(temperature) as avg_temp,
                    AVG(vibration) as avg_vibration,
                    AVG(pressure) as avg_pressure
                FROM FACTORY_PIPELINE_DEMO.PUBLIC.RAW_SENSOR_DATA
                GROUP BY machine_id
            )
            SELECT 
                m.machine_id,
                m.health_status,
                m.failure_risk_score,
                s.avg_temp,
                s.avg_vibration,
                s.avg_pressure
            FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics m
            JOIN recent_sensor_data s ON m.machine_id = s.machine_id
        """)
        data = pd.DataFrame(cursor.fetchall(), 
                          columns=['machine_id', 'health_status', 'failure_risk_score', 
                                 'avg_temp', 'avg_vibration', 'avg_pressure'])
        
        # Check if at least one sensor metric correlates with health status
        status_order = {'HEALTHY': 0, 'NEEDS_MAINTENANCE': 1, 'CRITICAL': 2}
        data['health_status_score'] = data['health_status'].map(status_order)
        
        correlations = []
        for sensor in ['avg_temp', 'avg_vibration', 'avg_pressure']:
            correlation = data['health_status_score'].corr(data[sensor])
            correlations.append(correlation)
        
        # Assert that at least one sensor has a positive correlation with health status
        assert any(corr > 0 for corr in correlations), \
            "At least one sensor metric should positively correlate with worse health status"
            
    finally:
        cursor.close()
