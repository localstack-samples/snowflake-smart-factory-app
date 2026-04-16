import boto3
import snowflake.connector

S3_ENDPOINT_URL = "http://localhost:4566"
S3_BUCKET_NAME = "factory-sensor-data-local"

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

PIPE_NAME = "SENSOR_DATA_PIPE"


def get_snowpipe_notification_arn():
    """Fetch the notification channel (SQS ARN) from the Snowpipe definition.

    LocalStack for Snowflake auto-provisions an SQS queue named
    ``sf-snowpipe-<ACCOUNT>`` when ``AUTO_INGEST = TRUE`` pipes are created.
    The account is normalised to upper-case, so hard-coding the ARN is
    fragile - always fetch it via ``DESC PIPE`` instead.
    """
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESC PIPE {PIPE_NAME}")
        columns = [col[0].lower() for col in cursor.description]
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError(f"Pipe '{PIPE_NAME}' not found - run 'make seed' first.")
        record = dict(zip(columns, row))
        notification_channel = record.get("notification_channel")
        if not notification_channel:
            raise RuntimeError(
                f"Pipe '{PIPE_NAME}' has no notification_channel; "
                "ensure it was created with AUTO_INGEST = TRUE."
            )
        return notification_channel
    finally:
        conn.close()


def create_s3_bucket():
    """Create the S3 bucket that will hold raw sensor data."""
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    try:
        s3.create_bucket(Bucket=S3_BUCKET_NAME)
        print(f"Bucket '{S3_BUCKET_NAME}' created successfully.")
    except Exception as e:
        print(f"Could not create bucket: {e}")


def configure_event_notification(sqs_queue_arn):
    """Configure S3 bucket notifications so Snowpipe auto-ingests new CSVs."""
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    notification_config = {
        "QueueConfigurations": [
            {
                "Id": "snowpipe-ingest-notification",
                "QueueArn": sqs_queue_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {"Name": "prefix", "Value": "raw_data/"},
                            {"Name": "suffix", "Value": ".csv"},
                        ]
                    }
                },
            }
        ]
    }

    try:
        s3.put_bucket_notification_configuration(
            Bucket=S3_BUCKET_NAME,
            NotificationConfiguration=notification_config,
        )
        print(
            f"Event notification configured for bucket '{S3_BUCKET_NAME}' "
            f"-> {sqs_queue_arn}"
        )
    except Exception as e:
        print(f"Could not configure event notification: {e}")
        raise


if __name__ == "__main__":
    create_s3_bucket()
    sqs_queue_arn = get_snowpipe_notification_arn()
    print(f"Resolved Snowpipe notification ARN: {sqs_queue_arn}")
    configure_event_notification(sqs_queue_arn)
    print("S3 bucket setup complete with Snowpipe notification configuration.")
