import boto3
import json

# Configuration
S3_ENDPOINT_URL = "http://localhost:4566"
S3_BUCKET_NAME = "factory-sensor-data-local"
SQS_QUEUE_ARN = "arn:aws:sqs:us-east-1:000000000000:sf-snowpipe-test"

def create_s3_bucket():
    """Create S3 bucket for sensor data"""
    s3 = boto3.client(
        "s3", 
        endpoint_url=S3_ENDPOINT_URL, 
        aws_access_key_id="test", 
        aws_secret_access_key="test", 
        region_name="us-east-1"
    )
    
    # Create bucket
    try:
        s3.create_bucket(Bucket=S3_BUCKET_NAME)
        print(f"Bucket '{S3_BUCKET_NAME}' created successfully.")
    except Exception as e:
        print(f"Could not create bucket: {e}")

def configure_event_notification():
    """Configure S3 bucket notifications for Snowpipe"""
    s3 = boto3.client(
        "s3", 
        endpoint_url=S3_ENDPOINT_URL, 
        aws_access_key_id="test", 
        aws_secret_access_key="test", 
        region_name="us-east-1"
    )
    
    # Configure bucket notification for Snowpipe
    notification_config = {
        "QueueConfigurations": [
            {
                "Id": "snowpipe-ingest-notification",
                "QueueArn": SQS_QUEUE_ARN,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "prefix",
                                "Value": "raw_data/"
                            },
                            {
                                "Name": "suffix",
                                "Value": ".csv"
                            }
                        ]
                    }
                }
            }
        ]
    }
    
    try:
        s3.put_bucket_notification_configuration(
            Bucket=S3_BUCKET_NAME,
            NotificationConfiguration=notification_config
        )
        print(f"Event notification configured for bucket '{S3_BUCKET_NAME}'")
    except Exception as e:
        print(f"Could not configure event notification: {e}")

if __name__ == "__main__":
    create_s3_bucket()
    configure_event_notification()
    print("S3 bucket setup complete with Snowpipe notification configuration.") 
