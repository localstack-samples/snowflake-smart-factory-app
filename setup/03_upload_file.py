import boto3
import time
import argparse

# Configuration
S3_ENDPOINT_URL = "http://localhost:4566"
S3_BUCKET_NAME = "factory-sensor-data-local"
DEFAULT_FILE_PATH = "data/sensor_data_batch_1.csv"

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
    
    # Upload file
    try:
        s3.upload_file(file_path, S3_BUCKET_NAME, target_filename)
        print(f"File '{file_path}' uploaded to '{S3_BUCKET_NAME}/{target_filename}'")
        print("Snowpipe should now automatically ingest this file into RAW_SENSOR_DATA table")
    except Exception as e:
        print(f"Could not upload file: {e}")

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
    
    args = parser.parse_args()
    
    upload_file_to_s3(args.file, args.name)
    list_bucket_contents()
