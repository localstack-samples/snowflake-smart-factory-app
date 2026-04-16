import argparse
import glob
import os
import re
import time

import boto3
import snowflake.connector

S3_ENDPOINT_URL = "http://localhost:4566"
S3_BUCKET_NAME = "factory-sensor-data-local"
DEFAULT_FILE_PATH = "data/sensor_data_batch_1.csv"

SNOWFLAKE_CONFIG = {
    "account": "test",
    "user": "test",
    "password": "test",
    "database": "FACTORY_PIPELINE_DEMO",
    "schema": "PUBLIC",
    "host": "snowflake.localhost.localstack.cloud",
    "port": 4566,
    "protocol": "https",
    "warehouse": "test",
    "role": "test",
}

def find_latest_batch_file(data_dir="data"):
    """Find the batch file with the highest number"""
    if not os.path.exists(data_dir):
        print(f"Data directory '{data_dir}' does not exist")
        return None
    
    batch_files = glob.glob(os.path.join(data_dir, "sensor_data_batch_*.csv"))
    if not batch_files:
        print(f"No batch files found in '{data_dir}'")
        return None
    
    highest_batch_num = 0
    latest_file = None
    
    for f in batch_files:
        match = re.search(r"sensor_data_batch_(\d+).csv", os.path.basename(f))
        if match:
            batch_num = int(match.group(1))
            if batch_num > highest_batch_num:
                highest_batch_num = batch_num
                latest_file = f
    
    if latest_file:
        print(f"Found latest batch file: {latest_file} (batch {highest_batch_num})")
        return latest_file
    else:
        print("No valid batch files found")
        return None

def upload_file_to_s3(file_path, custom_filename=None):
    """Upload a file to S3 which will trigger Snowpipe ingestion"""
    s3 = boto3.client(
        "s3", 
        endpoint_url=S3_ENDPOINT_URL, 
        aws_access_key_id="test", 
        aws_secret_access_key="test", 
        region_name="us-east-1"
    )
    
    # Generate target filename
    if custom_filename:
        target_filename = f"raw_data/{custom_filename}"
    else:
        # Add timestamp to filename to ensure uniqueness
        timestamp = int(time.time())
        base_filename = file_path.split('/')[-1]
        filename_parts = base_filename.split('.')
        if len(filename_parts) > 1:
            # Insert timestamp before extension
            new_name = f"{filename_parts[0]}_{timestamp}.{filename_parts[1]}"
        else:
            new_name = f"{base_filename}_{timestamp}"
        target_filename = f"raw_data/{new_name}"
    
    try:
        s3.upload_file(file_path, S3_BUCKET_NAME, target_filename)
        print(f"File '{file_path}' uploaded to '{S3_BUCKET_NAME}/{target_filename}'")
        print("Snowpipe should now automatically ingest this file into RAW_SENSOR_DATA table")
        return target_filename
    except Exception as e:
        print(f"Could not upload file: {e}")
        return None


def trigger_snowpipe_copy(uploaded_filename):
    """Execute a scoped COPY INTO as a workaround for a LocalStack bug.

    Real Snowflake runs the pipe's COPY automatically once S3 delivers a
    notification to its internal SQS queue. The LocalStack Snowflake
    emulator (tested on ``localstack/snowflake:latest`` as of 2026-04)
    consumes the SQS notifications, but the auto-generated COPY targets
    ``@stage/file.csv`` which returns zero rows in the emulator. To keep
    the demo end-to-end functional, we run the COPY ourselves and scope
    it to the file that was just uploaded via the ``PATTERN`` clause so
    running ``make upload`` multiple times does not re-load previous
    batches. Remove this helper once the upstream bug is fixed.
    """
    filename = os.path.basename(uploaded_filename)
    pattern = f".*{filename}"
    print(
        "Running scoped COPY INTO to emulate Snowpipe auto-ingest "
        f"(LocalStack workaround, pattern='{pattern}')..."
    )
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "COPY INTO RAW_SENSOR_DATA "
            "FROM @SENSOR_DATA_STAGE "
            f"PATTERN='{pattern}' "
            "ON_ERROR='CONTINUE'"
        )
        rows = cursor.fetchall()
        total_loaded = 0
        for row in rows:
            status = row[1] if len(row) > 1 else ""
            loaded = row[3] if len(row) > 3 else 0
            try:
                total_loaded += int(loaded or 0)
            except (TypeError, ValueError):
                pass
            print(f"  status={status}, rows_loaded={loaded}")
        print(f"Snowpipe COPY complete. New rows loaded: {total_loaded}")
    finally:
        conn.close()

def list_bucket_contents():
    """List contents of the S3 bucket"""
    s3 = boto3.client(
        "s3", 
        endpoint_url=S3_ENDPOINT_URL, 
        aws_access_key_id="test", 
        aws_secret_access_key="test", 
        region_name="us-east-1"
    )
    
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="raw_data/")
        if 'Contents' in response:
            print(f"\nFiles in {S3_BUCKET_NAME}/raw_data/:")
            for obj in response['Contents']:
                print(f"  - {obj['Key']} ({obj['Size']} bytes, last modified: {obj['LastModified']})")
        else:
            print(f"\nNo files found in {S3_BUCKET_NAME}/raw_data/")
    except Exception as e:
        print(f"Could not list bucket contents: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload a file to S3 to trigger Snowpipe ingestion')
    parser.add_argument('--file', type=str, default=DEFAULT_FILE_PATH,
                      help=f'Path to the file to upload (default: {DEFAULT_FILE_PATH})')
    parser.add_argument('--name', type=str,
                      help='Custom filename to use in S3 (optional)')
    parser.add_argument('--latest', action='store_true',
                      help='Upload the latest (highest numbered) batch file instead of batch 1')
    
    args = parser.parse_args()
    
    # Determine which file to upload
    if args.latest:
        file_to_upload = find_latest_batch_file()
        if file_to_upload is None:
            print("Error: Could not find latest batch file. Exiting.")
            exit(1)
    else:
        file_to_upload = args.file
    
    uploaded_key = upload_file_to_s3(file_to_upload, args.name)
    list_bucket_contents()
    if uploaded_key:
        trigger_snowpipe_copy(uploaded_key)
