terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 1.0.0"
    }
  }
}

provider "snowflake" {
  organization_name = "test"
  account_name  = "test"
  user     = "test"
  password = "test"
  role     = "test"
  host     = "snowflake.localhost.localstack.cloud"
  port = "4566"
}

# Create the factory pipeline database
resource "snowflake_database" "factory_pipeline_demo" {
  name                        = "FACTORY_PIPELINE_DEMO"
  comment                     = "Factory pipeline demo database"
  data_retention_time_in_days = 3
}

# Create the public schema
resource "snowflake_schema" "public" {
  database            = snowflake_database.factory_pipeline_demo.name
  name                = "PUBLIC"
  comment             = "Public schema for factory pipeline demo"
  with_managed_access = false
}

# Create the raw sensor data table
resource "snowflake_table" "raw_sensor_data" {
  database = snowflake_database.factory_pipeline_demo.name
  schema   = snowflake_schema.public.name
  name     = "RAW_SENSOR_DATA"
  comment  = "Raw sensor data from factory machines"
  column {
    name = "machine_id"
    type = "VARCHAR(50)"
  }
  column {
    name = "timestamp"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "temperature"
    type = "FLOAT"
  }
  column {
    name = "vibration"
    type = "FLOAT"
  }
  column {
    name = "pressure"
    type = "FLOAT"
  }
  column {
    name = "status_code"
    type = "VARCHAR(10)"
  }
}

# Create CSV file format
resource "snowflake_file_format" "csv_format" {
  database = snowflake_database.factory_pipeline_demo.name
  schema   = snowflake_schema.public.name
  name     = "csv_format"
  format_type = "CSV"
  field_delimiter = ","
  skip_header = 1
  null_if = ["NULL", "null"]
  empty_field_as_null = true
  comment = "CSV file format for sensor data ingestion"
}

# Create S3 stage for sensor data
resource "snowflake_stage" "sensor_data_stage" {
  database = snowflake_database.factory_pipeline_demo.name
  schema   = snowflake_schema.public.name
  name     = "sensor_data_stage"
  url = "s3://factory-sensor-data-local/raw_data/"
  credentials = "AWS_KEY_ID='test' AWS_SECRET_KEY='test'"
  file_format = "DATABASE=${snowflake_database.factory_pipeline_demo.name}.SCHEMA=${snowflake_schema.public.name}.${snowflake_file_format.csv_format.name}"
  comment = "S3 stage for factory sensor data files"
}

# Create Snowpipe for automated ingestion
resource "snowflake_pipe" "sensor_data_pipe" {
  database = snowflake_database.factory_pipeline_demo.name
  schema   = snowflake_schema.public.name
  name     = "sensor_data_pipe"
  copy_statement = "COPY INTO ${snowflake_database.factory_pipeline_demo.name}.${snowflake_schema.public.name}.${snowflake_table.raw_sensor_data.name} FROM @${snowflake_stage.sensor_data_stage.name} PATTERN='.*[.]csv' ON_ERROR = 'CONTINUE'"
  auto_ingest = true
  comment = "Automated pipe for ingesting sensor data from S3"
}
