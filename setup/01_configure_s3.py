import boto3
S3_ENDPOINT_URL = "http://localhost:4566"
S3_BUCKET_NAME = "factory-sensor-data-local"
LOCAL_FILE_PATH = "data/sensor_data_batch_1.csv"

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL, aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")

# Create bucket
try:
    s3.create_bucket(Bucket=S3_BUCKET_NAME)
    print(f"Bucket '{S3_BUCKET_NAME}' created successfully.")
except Exception as e:
    print(f"Could not create bucket: {e}")

# Upload file to raw_data/ prefix
try:
    s3.upload_file(LOCAL_FILE_PATH, S3_BUCKET_NAME, "raw_data/sensor_data_batch_1.csv")
    print(f"File '{LOCAL_FILE_PATH}' uploaded to '{S3_BUCKET_NAME}/raw_data/sensor_data_batch_1.csv'.")
except Exception as e:
    print(f"Could not upload file: {e}")
