import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
import numpy as np
from datetime import datetime, timedelta

def load_machine_health_data(conn, show_debug=False):
    """Load machine health data from Snowflake"""
    try:
        # Get the data using SQL query with correct schema name
        query = "SELECT * FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics"
        
        # Execute query using Snowflake cursor
        cur = conn.cursor()
        cur.execute(query)
        
        # Get column names from cursor description
        columns = [desc[0].lower() for desc in cur.description]
        if show_debug:
            st.write("Debug - Available columns:", columns)
        
        # Fetch all data and create DataFrame
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        # Debug output
        if show_debug:
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

def create_gauge_chart(value, title, min_val, max_val, threshold_ranges):
    """Create a gauge chart for sensor readings"""
    colors = ['#00ff00', '#ffa500', '#ff0000']
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = value,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': title},
        gauge = {
            'axis': {'range': [min_val, max_val]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': threshold_ranges[0], 'color': colors[0]},
                {'range': threshold_ranges[1], 'color': colors[1]},
                {'range': threshold_ranges[2], 'color': colors[2]}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': threshold_ranges[1][1]
            }
        }
    ))
    fig.update_layout(height=200, margin=dict(l=10, r=10, t=50, b=10))
    return fig

def create_time_series(df, machine_id, metric, anomaly_threshold=None):
    """Create an interactive time series chart with anomaly detection"""
    machine_data = df[df['machine_id'] == machine_id].copy()
    machine_data['timestamp'] = pd.to_datetime(machine_data['timestamp'])
    
    # Calculate rolling mean and std for anomaly detection
    machine_data['rolling_mean'] = machine_data[metric].rolling(window=20).mean()
    machine_data['rolling_std'] = machine_data[metric].rolling(window=20).std()
    
    # Identify anomalies
    if anomaly_threshold:
        machine_data['is_anomaly'] = abs(machine_data[metric] - machine_data['rolling_mean']) > (anomaly_threshold * machine_data['rolling_std'])
    
    # Create base line chart
    fig = go.Figure()
    
    # Add main metric line
    fig.add_trace(go.Scatter(
        x=machine_data['timestamp'],
        y=machine_data[metric],
        name=metric.title(),
        mode='lines',
        line=dict(color='blue'),
        hovertemplate=
        '<b>Time</b>: %{x}<br>' +
        '<b>Value</b>: %{y:.2f}<br>'
    ))
    
    # Add anomaly points if detected
    if anomaly_threshold and machine_data['is_anomaly'].any():
        anomalies = machine_data[machine_data['is_anomaly']]
        fig.add_trace(go.Scatter(
            x=anomalies['timestamp'],
            y=anomalies[metric],
            mode='markers',
            name='Anomalies',
            marker=dict(color='red', size=8, symbol='circle'),
            hovertemplate=
            '<b>Anomaly</b><br>' +
            '<b>Time</b>: %{x}<br>' +
            '<b>Value</b>: %{y:.2f}<br>'
        ))
    
    fig.update_layout(
        title=f"{metric.title()} Over Time - Machine {machine_id}",
        xaxis_title="Time",
        yaxis_title=metric.title(),
        hovermode='x unified',
        showlegend=True,
        height=500,
        margin=dict(l=10, r=10, t=30, b=10)
    )
    
    return fig

def main():
    st.set_page_config(
        page_title="Smart Factory Health Monitor",
        page_icon="🏭",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("🏭 Smart Factory Health Monitor")
    st.markdown("Real-time monitoring and predictive maintenance for industrial equipment")

    # Sidebar for navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a view:",
        ["Dashboard", "Machine Details", "Alerts & Notifications", "Data Pipeline"]
    )

    if page == "Dashboard":
        show_dashboard()
    elif page == "Machine Details":
        show_machine_details()
    elif page == "Alerts & Notifications":
        show_alerts_notifications()
    elif page == "Data Pipeline":
        show_data_pipeline()

def show_dashboard():
    # Debug toggle
    show_debug = st.checkbox("Show Debug Info", value=False)

    try:
        # Create Snowflake connection
        conn = st.connection("snowflake")
        # conn = get_active_session()
        
        # Debug connection info
        if show_debug:
            st.write("Debug - Connection established:", bool(conn))
        
        # Load data
        health_data = load_machine_health_data(conn, show_debug)
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
        
        # Overview metrics
        st.subheader("📊 Factory Overview")
        overview_cols = st.columns(4)
        
        total_machines = len(health_data['machine_id'].unique())
        healthy_machines = len(health_data[health_data['health_status'] == 'HEALTHY'])
        critical_machines = len(health_data[health_data['health_status'] == 'CRITICAL'])
        
        overview_cols[0].metric("Total Machines", total_machines)
        overview_cols[1].metric(
            "Healthy Machines", 
            healthy_machines, 
            delta=f"{(healthy_machines/total_machines)*100:.1f}%",
            delta_color="normal"
        )
        overview_cols[2].metric(
            "Critical Machines", 
            critical_machines,
            delta=f"{(critical_machines/total_machines)*100:.1f}%",
            delta_color="inverse"
        )
        overview_cols[3].metric(
            "Average Risk Score", 
            f"{health_data['failure_risk_score'].mean():.2f}",
            delta_color="normal"
        )

        # Dashboard layout
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔄 Machine Health Status")
            status_counts = health_data['health_status'].value_counts()
            fig = px.pie(values=status_counts.values, 
                        names=status_counts.index, 
                        title="Health Status Distribution",
                        color_discrete_map={
                            'HEALTHY': '#00ff00',
                            'NEEDS_MAINTENANCE': '#ffa500',
                            'CRITICAL': '#ff0000'
                        })
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            st.subheader("⚠️ Risk Analysis")
            fig = px.bar(health_data, 
                        x='machine_id', 
                        y='failure_risk_score',
                        color='health_status',
                        title="Failure Risk Scores by Machine",
                        color_discrete_map={
                            'HEALTHY': '#00ff00',
                            'NEEDS_MAINTENANCE': '#ffa500',
                            'CRITICAL': '#ff0000'
                        })
            fig.update_layout(xaxis_title="Machine ID", 
                             yaxis_title="Risk Score",
                             hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed machine data
        st.subheader("🔍 Machine Details")
        
        # Machine selector
        selected_machine = st.selectbox(
            "Select Machine for Detailed View",
            options=health_data['machine_id'].unique(),
            format_func=lambda x: f"Machine {x}"
        )
        
        if selected_machine:
            machine_health = health_data[health_data['machine_id'] == selected_machine].iloc[0]
            machine_sensors = sensor_data[sensor_data['machine_id'] == selected_machine].iloc[-1]
            
            # Status indicators
            status_cols = st.columns(5)  # Changed from 4 to 5 columns
            
            # Health Status with color-coded delta
            status_color = {
                'HEALTHY': 'normal',
                'NEEDS_MAINTENANCE': 'inverse',
                'CRITICAL': 'inverse'
            }
            
            # Determine risk level based on health status and risk score
            risk_score = float(machine_health['failure_risk_score'])
            
            # Align risk assessment with health status
            if machine_health['health_status'] == 'HEALTHY':
                risk_delta = "Low Risk"
                delta_color = 'normal'
            elif machine_health['health_status'] == 'NEEDS_MAINTENANCE':
                risk_delta = "Medium Risk"
                delta_color = 'inverse'
            else:  # CRITICAL
                risk_delta = "High Risk"
                delta_color = 'inverse'
            
            status_cols[0].metric(
                "Health Status",
                machine_health['health_status'],
                delta="Current Status",
                delta_color=status_color.get(machine_health['health_status'], 'normal')
            )
            
            status_cols[1].metric(
                "Risk Score",
                f"{risk_score:.2f}",
                delta=risk_delta,
                delta_color=delta_color
            )
            
            # Calculate temperature delta and determine color
            temp_delta = machine_sensors['temperature'] - sensor_data[sensor_data['machine_id'] == selected_machine]['temperature'].mean()
            temp_delta_color = 'inverse' if abs(temp_delta) > 5 else 'normal'
            
            status_cols[2].metric(
                "Temperature",
                f"{machine_sensors['temperature']:.1f}°C",
                delta=f"{temp_delta:.1f}°C",
                delta_color=temp_delta_color
            )
            
            # Calculate pressure delta and determine color
            pressure_delta = machine_sensors['pressure'] - sensor_data[sensor_data['machine_id'] == selected_machine]['pressure'].mean()
            pressure_delta_color = 'inverse' if abs(pressure_delta) > 10 else 'normal'
            
            status_cols[3].metric(
                "Pressure",
                f"{machine_sensors['pressure']:.1f}",
                delta=f"{pressure_delta:.1f}",
                delta_color=pressure_delta_color
            )
            
            # Calculate vibration delta and determine color
            vib_delta = machine_sensors['vibration'] - sensor_data[sensor_data['machine_id'] == selected_machine]['vibration'].mean()
            vib_delta_color = 'inverse' if abs(vib_delta) > 0.1 else 'normal'
            
            status_cols[4].metric(
                "Vibration",
                f"{machine_sensors['vibration']:.3f}",
                delta=f"{vib_delta:.3f}",
                delta_color=vib_delta_color
            )
            
            # Maintenance recommendation
            if machine_health['maintenance_recommendation']:
                st.info(f"📋 Recommendation: {machine_health['maintenance_recommendation']}")
            
            # Sensor gauges
            gauge_cols = st.columns(3)
            
            # Temperature gauge
            temp_ranges = [(0, 50), (50, 75), (75, 100)]
            temp_gauge = create_gauge_chart(
                machine_sensors['temperature'],
                "Temperature (°C)",
                0, 100,
                temp_ranges
            )
            gauge_cols[0].plotly_chart(temp_gauge, use_container_width=True)
            
            # Pressure gauge
            pressure_ranges = [(0, 100), (100, 150), (150, 200)]
            pressure_gauge = create_gauge_chart(
                machine_sensors['pressure'],
                "Pressure",
                0, 200,
                pressure_ranges
            )
            gauge_cols[1].plotly_chart(pressure_gauge, use_container_width=True)
            
            # Vibration gauge
            vibration_ranges = [(0, 0.5), (0.5, 0.8), (0.8, 1.0)]
            vibration_gauge = create_gauge_chart(
                machine_sensors['vibration'],
                "Vibration",
                0, 1,
                vibration_ranges
            )
            gauge_cols[2].plotly_chart(vibration_gauge, use_container_width=True)
            
            # Time series charts with anomaly detection
            st.subheader("📈 Sensor Trends")
            
            # Create tabs for different sensor metrics
            trend_tabs = st.tabs(["Temperature", "Pressure", "Vibration"])
            
            with trend_tabs[0]:
                temp_chart = create_time_series(sensor_data, selected_machine, 'temperature', anomaly_threshold=2)
                st.plotly_chart(temp_chart, use_container_width=True)
                
            with trend_tabs[1]:
                pressure_chart = create_time_series(sensor_data, selected_machine, 'pressure', anomaly_threshold=2)
                st.plotly_chart(pressure_chart, use_container_width=True)
                
            with trend_tabs[2]:
                vib_chart = create_time_series(sensor_data, selected_machine, 'vibration', anomaly_threshold=2)
                st.plotly_chart(vib_chart, use_container_width=True)

    except Exception as e:
        st.error(f"Error in application: {str(e)}")
        st.info("Make sure LocalStack is running and the Snowflake emulator is properly configured.")

def show_machine_details():
    # Debug toggle
    show_debug = st.checkbox("Show Debug Info", value=False)

    try:
        # Create Snowflake connection
        conn = st.connection("snowflake")
        # conn = get_active_session()
        
        # Debug connection info
        if show_debug:
            st.write("Debug - Connection established:", bool(conn))
        
        # Load data
        health_data = load_machine_health_data(conn, show_debug)
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
        
        # Detailed machine data
        st.subheader("🔍 Machine Details")
        
        # Machine selector
        selected_machine = st.selectbox(
            "Select Machine for Detailed View",
            options=health_data['machine_id'].unique(),
            format_func=lambda x: f"Machine {x}"
        )
        
        if selected_machine:
            machine_health = health_data[health_data['machine_id'] == selected_machine].iloc[0]
            machine_sensors = sensor_data[sensor_data['machine_id'] == selected_machine].iloc[-1]
            
            # Status indicators
            status_cols = st.columns(5)  # Changed from 4 to 5 columns
            
            # Health Status with color-coded delta
            status_color = {
                'HEALTHY': 'normal',
                'NEEDS_MAINTENANCE': 'inverse',
                'CRITICAL': 'inverse'
            }
            
            # Determine risk level based on health status and risk score
            risk_score = float(machine_health['failure_risk_score'])
            
            # Align risk assessment with health status
            if machine_health['health_status'] == 'HEALTHY':
                risk_delta = "Low Risk"
                delta_color = 'normal'
            elif machine_health['health_status'] == 'NEEDS_MAINTENANCE':
                risk_delta = "Medium Risk"
                delta_color = 'inverse'
            else:  # CRITICAL
                risk_delta = "High Risk"
                delta_color = 'inverse'
            
            status_cols[0].metric(
                "Health Status",
                machine_health['health_status'],
                delta="Current Status",
                delta_color=status_color.get(machine_health['health_status'], 'normal')
            )
            
            status_cols[1].metric(
                "Risk Score",
                f"{risk_score:.2f}",
                delta=risk_delta,
                delta_color=delta_color
            )
            
            # Calculate temperature delta and determine color
            temp_delta = machine_sensors['temperature'] - sensor_data[sensor_data['machine_id'] == selected_machine]['temperature'].mean()
            temp_delta_color = 'inverse' if abs(temp_delta) > 5 else 'normal'
            
            status_cols[2].metric(
                "Temperature",
                f"{machine_sensors['temperature']:.1f}°C",
                delta=f"{temp_delta:.1f}°C",
                delta_color=temp_delta_color
            )
            
            # Calculate pressure delta and determine color
            pressure_delta = machine_sensors['pressure'] - sensor_data[sensor_data['machine_id'] == selected_machine]['pressure'].mean()
            pressure_delta_color = 'inverse' if abs(pressure_delta) > 10 else 'normal'
            
            status_cols[3].metric(
                "Pressure",
                f"{machine_sensors['pressure']:.1f}",
                delta=f"{pressure_delta:.1f}",
                delta_color=pressure_delta_color
            )
            
            # Calculate vibration delta and determine color
            vib_delta = machine_sensors['vibration'] - sensor_data[sensor_data['machine_id'] == selected_machine]['vibration'].mean()
            vib_delta_color = 'inverse' if abs(vib_delta) > 0.1 else 'normal'
            
            status_cols[4].metric(
                "Vibration",
                f"{machine_sensors['vibration']:.3f}",
                delta=f"{vib_delta:.3f}",
                delta_color=vib_delta_color
            )
            
            # Maintenance recommendation
            if machine_health['maintenance_recommendation']:
                st.info(f"📋 Recommendation: {machine_health['maintenance_recommendation']}")
            
            # Sensor gauges
            gauge_cols = st.columns(3)
            
            # Temperature gauge
            temp_ranges = [(0, 50), (50, 75), (75, 100)]
            temp_gauge = create_gauge_chart(
                machine_sensors['temperature'],
                "Temperature (°C)",
                0, 100,
                temp_ranges
            )
            gauge_cols[0].plotly_chart(temp_gauge, use_container_width=True)
            
            # Pressure gauge
            pressure_ranges = [(0, 100), (100, 150), (150, 200)]
            pressure_gauge = create_gauge_chart(
                machine_sensors['pressure'],
                "Pressure",
                0, 200,
                pressure_ranges
            )
            gauge_cols[1].plotly_chart(pressure_gauge, use_container_width=True)
            
            # Vibration gauge
            vibration_ranges = [(0, 0.5), (0.5, 0.8), (0.8, 1.0)]
            vibration_gauge = create_gauge_chart(
                machine_sensors['vibration'],
                "Vibration",
                0, 1,
                vibration_ranges
            )
            gauge_cols[2].plotly_chart(vibration_gauge, use_container_width=True)
            
            # Time series charts with anomaly detection
            st.subheader("📈 Sensor Trends")
            
            # Create tabs for different sensor metrics
            trend_tabs = st.tabs(["Temperature", "Pressure", "Vibration"])
            
            with trend_tabs[0]:
                temp_chart = create_time_series(sensor_data, selected_machine, 'temperature', anomaly_threshold=2)
                st.plotly_chart(temp_chart, use_container_width=True)
                
            with trend_tabs[1]:
                pressure_chart = create_time_series(sensor_data, selected_machine, 'pressure', anomaly_threshold=2)
                st.plotly_chart(pressure_chart, use_container_width=True)
                
            with trend_tabs[2]:
                vib_chart = create_time_series(sensor_data, selected_machine, 'vibration', anomaly_threshold=2)
                st.plotly_chart(vib_chart, use_container_width=True)

    except Exception as e:
        st.error(f"Error in application: {str(e)}")
        st.info("Make sure LocalStack is running and the Snowflake emulator is properly configured.")

def show_alerts_notifications():
    """Display real-time alerts and SES email notifications"""
    st.header("🚨 Real-time Alerts & SES Notifications")
    
    # Create tabs for different alert views
    tab1, tab2, tab3, tab4 = st.tabs(["🚨 Critical Alerts", "📧 Email Status", "📋 Alert Log", "⚙️ Settings"])
    
    with tab1:
        st.subheader("🚨 Critical Machine Monitor")
        st.markdown("Monitor critical machines and send email alerts using Python UDF + SES")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔍 Check Critical Machines", type="primary"):
                with st.spinner("Querying critical machines..."):
                    try:
                        # Query critical machines directly
                        query = """
                        SELECT 
                            machine_id,
                            health_status,
                            failure_risk_score,
                            maintenance_recommendation
                        FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics
                        WHERE health_status = 'CRITICAL'
                        ORDER BY failure_risk_score DESC
                        """
                        
                        df = execute_query(query)
                        
                        if not df.empty:
                            st.error(f"⚠️ Found {len(df)} machines in CRITICAL status!")
                            
                            # Color code by health status
                            def color_health_status(val):
                                if val == 'CRITICAL':
                                    return 'background-color: #ffebee; color: #d32f2f; font-weight: bold'
                                elif val == 'WARNING':
                                    return 'background-color: #fff3e0; color: #f57c00; font-weight: bold'
                                else:
                                    return 'background-color: #e8f5e8; color: #2e7d32; font-weight: bold'
                            
                            styled_df = df.style.applymap(color_health_status, subset=['health_status'])
                            st.dataframe(styled_df, use_container_width=True)
                            
                        else:
                            st.success("✅ No machines in CRITICAL status!")
                            
                    except Exception as e:
                        st.error(f"Error querying critical machines: {str(e)}")
        
        with col2:
            if st.button("📧 Send Alert Email", type="primary"):
                with st.spinner("Sending critical machines alert email..."):
                    try:
                        # Call the simple UDF to send alert email
                        query = "SELECT send_critical_machines_report()"
                        result = execute_query(query)
                        
                        if not result.empty:
                            # Parse the result
                            result_data = result.iloc[0, 0] if len(result.columns) > 0 else {}
                            
                            if isinstance(result_data, str):
                                try:
                                    import json
                                    result_data = json.loads(result_data)
                                except:
                                    pass
                            
                            if result_data.get('email_sent'):
                                st.success(f"✅ Alert email sent successfully!")
                                st.info(f"📧 **From:** {result_data.get('sender', 'N/A')}")
                                st.info(f"📧 **To:** {result_data.get('recipient', 'N/A')}")
                                st.info(f"📧 **Message ID:** {result_data.get('message_id', 'N/A')}")
                                st.info(f"🚨 **Critical Machines:** {result_data.get('total_critical_machines', 0)}")
                            else:
                                st.warning(f"⚠️ Email sending failed: {result_data.get('error', 'Unknown error')}")
                                
                            # Show the full result for debugging
                            with st.expander("📋 Full Result Details"):
                                st.json(result_data)
                        else:
                            st.error("No result returned from UDF")
                            
                    except Exception as e:
                        st.error(f"Error sending alert: {str(e)}")
    
    with tab2:
        st.subheader("📧 SES Email Status")
        st.markdown("Monitor email alerts sent via Amazon SES")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📬 Check Sent Emails"):
                with st.spinner("Retrieving sent emails from SES..."):
                    try:
                        import requests
                        import json
                        
                        # Query LocalStack SES endpoint
                        response = requests.get("http://localhost:4566/_aws/ses")
                        
                        if response.status_code == 200:
                            email_data = response.json()
                            messages = email_data.get('messages', [])
                            
                            if messages:
                                st.success(f"Found {len(messages)} sent emails")
                                
                                for i, msg in enumerate(messages):
                                    with st.expander(f"📧 Email {i+1}: {msg.get('Subject', 'No Subject')}"):
                                        col_a, col_b = st.columns(2)
                                        
                                        with col_a:
                                            st.write("**From:**", msg.get('Source', 'N/A'))
                                            st.write("**To:**", ', '.join(msg.get('Destination', {}).get('ToAddresses', [])))
                                            st.write("**Timestamp:**", msg.get('Timestamp', 'N/A'))
                                            st.write("**Message ID:**", msg.get('Id', 'N/A'))
                                        
                                        with col_b:
                                            st.write("**Subject:**", msg.get('Subject', 'N/A'))
                                            body = msg.get('Body', {})
                                            text_part = body.get('text_part', 'No content')
                                            st.text_area("Email Content:", text_part, height=150, key=f"email_{i}")
                            else:
                                st.info("No emails sent yet. Process some critical alerts first.")
                        else:
                            st.error(f"Failed to retrieve emails: HTTP {response.status_code}")
                            
                    except Exception as e:
                        st.error(f"Error checking emails: {str(e)}")
        
        with col2:
            if st.button("🗑️ Clear Email History"):
                try:
                    import requests
                    response = requests.delete("http://localhost:4566/_aws/ses")
                    if response.status_code == 200:
                        st.success("Email history cleared")
                    else:
                        st.error("Failed to clear email history")
                except Exception as e:
                    st.error(f"Error clearing emails: {str(e)}")
    
    with tab3:
        st.subheader("📋 Alert Log")
        st.markdown("Historical log of all processed alerts")
        
        if st.button("🔄 Refresh Alert Log"):
            with st.spinner("Loading alert log..."):
                try:
                    query = """
                    SELECT 
                        alert_id,
                        machine_id,
                        risk_score,
                        recommendation,
                        alert_message,
                        email_sent,
                        action_type,
                        alert_timestamp
                    FROM ALERT_LOG 
                    ORDER BY alert_timestamp DESC
                    LIMIT 50
                    """
                    
                    df = execute_query(query)
                    
                    if not df.empty:
                        st.success(f"Showing {len(df)} recent alerts")
                        
                        # Color code by email status
                        def color_email_status(val):
                            if val:
                                return 'background-color: #e8f5e8; color: #2e7d32'
                            else:
                                return 'background-color: #ffebee; color: #d32f2f'
                        
                        styled_df = df.style.applymap(color_email_status, subset=['email_sent'])
                        st.dataframe(styled_df, use_container_width=True)
                        
                        # Summary stats
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Alerts", len(df))
                        with col2:
                            emails_sent = df['email_sent'].sum() if 'email_sent' in df.columns else 0
                            st.metric("Emails Sent", emails_sent)
                        with col3:
                            unique_machines = df['machine_id'].nunique() if 'machine_id' in df.columns else 0
                            st.metric("Affected Machines", unique_machines)
                    else:
                        st.info("No alerts in log. Process some critical alerts first.")
                        
                except Exception as e:
                    st.error(f"Error loading alert log: {str(e)}")
    
    with tab4:
        st.subheader("⚙️ Alert Settings")
        st.markdown("Configure alerting system settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**SES Configuration**")
            st.code("""
Endpoint: http://localhost:4566
Region: us-east-1
Sender: factory-alerts@smartfactory.com
Recipients: 
  - maintenance-team@smartfactory.com
  - ops-team@smartfactory.com
            """)
            
            if st.button("🧪 Test Alert Email"):
                with st.spinner("Testing alert email..."):
                    try:
                        query = "SELECT send_critical_machines_report()"
                        result = execute_query(query)
                        st.success("Alert email test successful")
                        st.json(result.to_dict('records')[0] if not result.empty else {})
                    except Exception as e:
                        st.error(f"Alert email test failed: {str(e)}")
        
        with col2:
            st.write("**Alert Function Status**")
            try:
                query = "SHOW FUNCTIONS LIKE 'send_critical_machines_report'"
                functions_df = execute_query(query)
                if not functions_df.empty:
                    st.success("✅ Alert function is deployed")
                    st.dataframe(functions_df, use_container_width=True)
                else:
                    st.warning("⚠️ Alert function not found. Run 'make setup-alerts'")
            except:
                st.warning("Could not retrieve function information")
            
            st.write("**Alert Thresholds**")
            st.info("""
            Critical Conditions:
            • Temperature > 90°C
            • Vibration > 0.8
            • Risk Score > 80%
            """)

def show_data_pipeline():
    """Display data pipeline status and controls"""
    st.header("🔄 Data Pipeline Status")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Pipeline Components")
        
        # Pipeline status indicators
        components = [
            ("S3 Data Lake", "✅ Active"),
            ("Snowflake Database", "✅ Connected"),
            ("dbt Transformations", "✅ Running"),
            ("SES Email Service", "📧 Ready"),
            ("Python UDFs", "🐍 Deployed"),
            ("Alert Processing", "🚨 Ready")
        ]
        
        for component, status in components:
            st.write(f"**{component}:** {status}")
    
    with col2:
        st.subheader("🚨 Alert Pipeline")
        
        if st.button("🔄 Refresh Pipeline Status"):
            with st.spinner("Checking pipeline status..."):
                try:
                    # Check if UDF exists
                    query = "SHOW FUNCTIONS LIKE 'send_critical_machines_report'"
                    functions = execute_query(query)
                    
                    if not functions.empty:
                        st.success("✅ Alert UDF is deployed")
                    else:
                        st.warning("⚠️ Alert UDF not found. Run 'make setup-alerts'")
                        
                except Exception as e:
                    st.error(f"Error checking pipeline: {str(e)}")

def execute_query(query):
    """Execute a query using the Snowflake connection"""
    try:
        conn = st.connection("snowflake")
        return conn.query(query)
    except Exception as e:
        st.error(f"Query execution failed: {str(e)}")
        return pd.DataFrame()

# Custom CSS
st.markdown("""
    <style>
    .stMetric {
        background-color: #1E2022;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #2E3236;
        min-height: 120px;  /* Fixed height for all metric cards */
    }
    .stMetric:hover {
        background-color: #2E3236;
        border-color: #3E4246;
    }
    .stMetric [data-testid="stMetricLabel"] {
        color: #E0E2E6 !important;
        font-size: 1rem !important;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #FFFFFF !important;
        font-size: 2rem !important;
    }
    .stMetric [data-testid="stMetricDelta"] {
        color: #B0B2B6 !important;
    }
    .stProgress .st-bo {
        background-color: #00ff00;
    }

    /* Footer styling */
    .footer-container {
        margin-top: 1rem;
        padding-bottom: 0;
    }
    .footer {
        background-color: rgba(30, 32, 34, 0.8);
        padding: 0.5rem;
        border-radius: 4px;
        border: 1px solid #2E3236;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 0.5rem;
        margin: 0;
    }
    
    /* Hide any overflow at the bottom of the page */
    .main .block-container {
        padding-bottom: 0;
        margin-bottom: 0;
    }

    /* Hide default footer */
    footer {visibility: hidden;}
    
    /* Custom footer */
    .custom-footer {
        position: fixed;
        bottom: 0;
        left: 0;
        width: 100%;
        background-color: #0E1117;
        padding: 8px 0;
        text-align: center;
        z-index: 999;
    }
    .custom-footer p {
        margin: 0;
        color: #FAFAFA;
        font-size: 14px;
        line-height: 1.4;
    }
    /* Adjust main content to not be hidden by footer */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        gap: 1rem;
    }

    /* Reduce spacing between elements */
    .element-container {
        margin-bottom: 1rem !important;  /* Reduced from default */
    }
    
    /* Reduce tab padding */
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
        padding: 0.5rem 0;
    }
    
    /* Adjust plot container margins */
    .stPlotlyChart {
        margin-bottom: 0.5rem;
    }
    </style>
    """, unsafe_allow_html=True)

# Move this outside the try-except block, at the very end of the file
st.markdown("""
    <div class="custom-footer">
        <p>Built with ❤️ by LocalStack</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 
