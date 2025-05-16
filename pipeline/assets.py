from dagster import asset, AssetExecutionContext, Definitions, load_assets_from_modules, sensor, RunRequest, DefaultSensorStatus
from dagster_aws.s3 import S3Resource
from dagster_dbt import DbtCliResource, dbt_assets
from typing import List
import os

# Configure dbt project
DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_project")
DBT_PROFILES_DIR = DBT_PROJECT_DIR

@dbt_assets(manifest=os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"))
def factory_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Run dbt transformations for factory data pipeline
    """
    yield from dbt.cli(
        ["build"],
        context=context
    ).stream()

@asset(
    group_name="factory_data",
    description="Monitor S3 bucket for new sensor data files"
)
def monitor_sensor_data(s3: S3Resource) -> List[str]:
    """
    Monitor the S3 bucket for new sensor data files
    """
    bucket = "factory-sensor-data-local"
    prefix = "raw_data/"
    
    # Get S3 client
    s3_client = s3.get_client()
    
    # List objects in the bucket
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    
    # Get list of files
    files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.csv'):
                files.append(key)
    
    return files

@sensor(
    job_name="__ASSET_JOB",
    minimum_interval_seconds=15,
    default_status=DefaultSensorStatus.RUNNING
)
def s3_file_sensor(context, s3: S3Resource):
    """
    Sensor that monitors S3 bucket for new files and triggers the pipeline
    """
    bucket = "factory-sensor-data-local"
    prefix = "raw_data/"
    
    s3_client = s3.get_client()
    last_processed = context.cursor or set()
    last_processed = set(last_processed.split(",")) if last_processed else set()
    
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    
    current_files = set()
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.csv'):
                current_files.add(key)
    
    new_files = current_files - last_processed
    
    if new_files:
        context.update_cursor(",".join(current_files))
        
        context.log.info(f"Found new files: {new_files}")
        return RunRequest(
            run_key=f"s3_files_{'_'.join(sorted(new_files))}",
            run_config={}
        )
    
    return None

# Define resources
resources = {
    "s3": S3Resource(
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
    ),
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )
}

# Create definitions
defs = Definitions(
    assets=[monitor_sensor_data, factory_dbt_assets],
    sensors=[s3_file_sensor],
    resources=resources
) 
