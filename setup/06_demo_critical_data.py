#!/usr/bin/env python3
"""
Demo script to generate critical sensor data that triggers stream-based SES alerts.
This simulates machines going from HEALTHY to CRITICAL status.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import boto3
from botocore.config import Config

def generate_critical_sensor_data():
    """Generate sensor data that will trigger CRITICAL health status"""
    
    # Set random seed for reproducible demo
    np.random.seed(42)
    
    # Generate timestamps for the last hour
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=1)
    
    # Create time series (every 5 minutes = 12 readings)
    timestamps = pd.date_range(start=start_time, end=end_time, freq='5min')
    
    # Define machines that will go critical
    critical_machines = ['MACHINE_001', 'MACHINE_003', 'MACHINE_007']
    
    data = []
    
    for machine_id in critical_machines:
        for i, timestamp in enumerate(timestamps):
            # Gradually increase temperature and vibration to critical levels
            progress = i / len(timestamps)  # 0 to 1
            
            # Temperature: start normal (70-75), end critical (95-100)
            base_temp = 72 + (25 * progress)  # 72 -> 97
            temperature = base_temp + np.random.normal(0, 2)
            
            # Vibration: start normal (0.3-0.4), end critical (0.85-0.95)
            base_vib = 0.35 + (0.6 * progress)  # 0.35 -> 0.95
            vibration = base_vib + np.random.normal(0, 0.05)
            
            # Pressure: some variation but not the main trigger
            pressure = 120 + np.random.normal(0, 10)
            
            # Status code: OK initially, then WARNING, then CRITICAL
            if progress < 0.3:
                status_code = 'OK'
            elif progress < 0.7:
                status_code = 'WARNING'
            else:
                status_code = 'CRITICAL'
            
            data.append({
                'machine_id': machine_id,
                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'temperature': round(temperature, 2),
                'vibration': round(vibration, 3),
                'pressure': round(pressure, 2),
                'status_code': status_code
            })
    
    return pd.DataFrame(data)

def upload_to_s3(df, filename='critical_sensor_data.csv'):
    """Upload the critical sensor data to S3"""
    
    # Save to CSV
    csv_path = f'/tmp/{filename}'
    df.to_csv(csv_path, index=False)
    
    # Configure S3 client for LocalStack
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1',
        config=Config(signature_version='s3v4')
    )
    
    try:
        # Upload to S3
        bucket_name = 'factory-sensor-data-local'
        s3_key = f'raw_data/{filename}'
        
        s3_client.upload_file(csv_path, bucket_name, s3_key)
        print(f"✅ Successfully uploaded {filename} to s3://{bucket_name}/{s3_key}")
        print(f"📊 Uploaded {len(df)} critical sensor readings")
        
        # Show sample of the data
        print("\n📋 Sample of critical data:")
        print(df.head(10).to_string(index=False))
        
        return True
        
    except Exception as e:
        print(f"❌ Error uploading to S3: {str(e)}")
        return False
    
    finally:
        # Clean up temp file
        if os.path.exists(csv_path):
            os.remove(csv_path)

def setup_ses_identities():
    """Setup SES email identities for the demo"""
    
    # Configure SES client for LocalStack
    ses_client = boto3.client(
        'ses',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    # Email identities to verify
    identities = [
        'factory-alerts@smartfactory.com',
        'maintenance-team@smartfactory.com',
        'ops-team@smartfactory.com'
    ]
    
    print("📧 Setting up SES email identities...")
    
    for email in identities:
        try:
            ses_client.verify_email_identity(EmailAddress=email)
            print(f"✅ Verified email identity: {email}")
        except Exception as e:
            print(f"⚠️  Warning: Could not verify {email}: {str(e)}")
    
    # List verified identities
    try:
        response = ses_client.list_identities()
        print(f"\n📋 Verified identities: {response.get('Identities', [])}")
        return True
    except Exception as e:
        print(f"❌ Error listing identities: {str(e)}")
        return False

def main():
    """Main function to generate critical data and setup SES"""
    print("🚨 Setting up Stream-based SES Alerting Demo...")
    
    # Setup SES identities first
    print("\n1️⃣ Setting up SES email identities...")
    ses_setup = setup_ses_identities()
    
    # Generate the critical data
    print("\n2️⃣ Generating critical sensor data...")
    critical_df = generate_critical_sensor_data()
    
    print(f"📈 Generated {len(critical_df)} sensor readings for {len(critical_df['machine_id'].unique())} machines")
    print(f"🌡️  Temperature range: {critical_df['temperature'].min():.1f}°C - {critical_df['temperature'].max():.1f}°C")
    print(f"📳 Vibration range: {critical_df['vibration'].min():.3f} - {critical_df['vibration'].max():.3f}")
    
    # Upload to S3
    print("\n3️⃣ Uploading to S3...")
    upload_success = upload_to_s3(critical_df)
    
    if upload_success and ses_setup:
        print("\n🎯 Demo ready! Next steps:")
        print("1. Run 'make dbt' to process the new data")
        print("2. Run 'snow sql -f setup/05_post_dbt_setup.sql -c localstack' to create stream")
        print("3. Query the stream: 'SELECT * FROM critical_alerts_processor'")
        print("4. Check SES emails: 'curl localhost:4566/_aws/ses | jq'")
        print("5. Watch real-time alerts with email notifications!")
        print("\n📧 Email alerts will be sent to: maintenance-team@smartfactory.com")
        print("🔍 Monitor LocalStack logs to see SES email sending activity")
    else:
        print("\n❌ Setup failed. Check LocalStack is running and try again.")

if __name__ == "__main__":
    main() 