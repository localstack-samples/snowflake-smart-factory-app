import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

def load_machine_health_data(conn):
    """Load machine health data from Snowflake"""
    try:
        # Get the data using SQL query with correct schema name
        query = "SELECT * FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics"
        
        # Execute query using Snowflake cursor
        cur = conn.cursor()
        cur.execute(query)
        
        # Get column names from cursor description
        columns = [desc[0].lower() for desc in cur.description]
        st.write("Debug - Available columns:", columns)
        
        # Fetch all data and create DataFrame
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        # Debug output
        st.write("Debug - Data shape:", df.shape)
        st.write("Debug - First few rows:", df.head())
        
        # Ensure all string columns are properly handled
        str_columns = ['machine_id', 'health_status', 'maintenance_recommendation']
        for col in str_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
            else:
                st.warning(f"Expected column '{col}' not found in data")
        
        return df
    except Exception as e:
        st.error(f"Error loading machine health data: {str(e)}")
        return pd.DataFrame()

def load_sensor_data(conn):
    """Load recent sensor data from Snowflake"""
    try:
        # Get the data using SQL query with correct schema name
        query = "SELECT * FROM FACTORY_PIPELINE_DEMO.PUBLIC.RAW_SENSOR_DATA ORDER BY timestamp DESC LIMIT 1000"
        
        # Execute query using Snowflake cursor
        cur = conn.cursor()
        cur.execute(query)
        
        # Get column names from cursor description
        columns = [desc[0].lower() for desc in cur.description]
        
        # Fetch all data and create DataFrame
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        # Ensure string columns are properly handled
        str_columns = ['machine_id', 'status_code']
        for col in str_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
            else:
                st.warning(f"Expected column '{col}' not found in data")
        
        return df
    except Exception as e:
        st.error(f"Error loading sensor data: {str(e)}")
        return pd.DataFrame()

# Page config
st.set_page_config(page_title="Smart Factory Monitor", layout="wide")
st.title("🏭 Smart Factory Health Monitor")

try:
    # Create Snowflake connection
    conn = st.connection("snowflake")
    # conn = get_active_session()
    
    # Debug connection info
    st.write("Debug - Connection established:", bool(conn))
    
    # Load data
    health_data = load_machine_health_data(conn)
    if health_data.empty:
        st.warning("No machine health data available.")
        st.stop()
    
    # Verify required columns exist
    required_columns = ['machine_id', 'health_status', 'failure_risk_score', 'maintenance_recommendation']
    missing_columns = [col for col in required_columns if col not in health_data.columns]
    if missing_columns:
        st.error(f"Missing required columns in health data: {missing_columns}")
        st.stop()
        
    sensor_data = load_sensor_data(conn)
    if sensor_data.empty:
        st.warning("No sensor data available.")
        st.stop()
    
    # Dashboard layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Machine Health Status")
        status_counts = health_data['health_status'].value_counts()
        fig = px.pie(values=status_counts.values, 
                    names=status_counts.index, 
                    title="Health Status Distribution",
                    color_discrete_map={
                        'HEALTHY': '#00ff00',
                        'NEEDS_MAINTENANCE': '#ffa500',
                        'CRITICAL': '#ff0000'
                    })
        st.plotly_chart(fig)
        
    with col2:
        st.subheader("Risk Scores by Machine")
        fig = px.bar(health_data, 
                    x='machine_id', 
                    y='failure_risk_score',
                    color='health_status',
                    title="Failure Risk Scores",
                    color_discrete_map={
                        'HEALTHY': '#00ff00',
                        'NEEDS_MAINTENANCE': '#ffa500',
                        'CRITICAL': '#ff0000'
                    })
        st.plotly_chart(fig)
    
    # Detailed machine data
    st.subheader("Machine Details")
    for machine_id in health_data['machine_id'].unique():
        with st.expander(f"Machine {machine_id}"):
            machine_health = health_data[health_data['machine_id'] == machine_id].iloc[0]
            machine_sensors = sensor_data[sensor_data['machine_id'] == machine_id].iloc[-1]
            
            cols = st.columns(4)
            cols[0].metric("Health Status", machine_health['health_status'])
            cols[1].metric("Risk Score", f"{machine_health['failure_risk_score']:.2f}")
            cols[2].metric("Temperature", f"{machine_sensors['temperature']:.1f}°C")
            cols[3].metric("Vibration", f"{machine_sensors['vibration']:.3f}")
            
            st.info(f"Recommendation: {machine_health['maintenance_recommendation']}")
            
            # Show historical sensor data
            machine_history = sensor_data[sensor_data['machine_id'] == machine_id]
            fig = px.line(machine_history, 
                         x='timestamp', 
                         y=['temperature', 'vibration', 'pressure'],
                         title=f"Sensor History - Machine {machine_id}")
            st.plotly_chart(fig)
    
except Exception as e:
    st.error(f"Error in application: {str(e)}")
    st.info("Make sure LocalStack is running and the Snowflake emulator is properly configured.") 
