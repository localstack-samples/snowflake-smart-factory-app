# Smart Factory Monitoring App

This project showcases a comprehensive smart factory monitoring application specifically designed to demonstrate **LocalStack for Snowflake** capabilities for local data cloud development, debugging, and testing throughout the entire software development lifecycle (SDLC).

This application showcases the full spectrum of **LocalStack for Snowflake** features, such as:

- Emulating a local Snowflake environment with databases, schemas, tables, and more.
- Utilizing S3 service for automated data ingestion with Snowpipe integration.
- Implementing dbt transformations for data quality and processing.
- Building interactive dashboards with Streamlit and Snowflake Native Apps.
- Orchestrating data transformations in an automated pipeline with Dagster.
- Implementing comprehensive data & integration testing with pytest and dbt tests.
- Using GitHub Actions for continuous integration to ensure automated testing.
- Adding real-time alerting with a Python User-Defined Function (UDFs) and Snowflake Tasks.
- Showcasing Zero-Copy Cloning for instant data recovery scenarios in case of data loss.

The application serves as a complete reference implementation for developing Snowflake data solutions locally, enabling real-time monitoring of factory machines, automated data ingestion, quality testing, and predictive maintenance alerts—all running entirely on your local machine through LocalStack's Snowflake emulator.

## Architecture

The application implements a modern data pipeline architecture showcasing a realistic smart factory scenario:

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│  S3 Bucket   │───▶│   Snowpipe      │
│  (CSV Sensors)  │    │ (LocalStack) │    │  (Auto-Ingest)  │
└─────────────────┘    └──────────────┘    └─────────────────┘
                                                     │
                                                     ▼
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Streamlit     │◀───│     dbt      │◀───│   Snowflake     │
│   Dashboard     │    │ Transformations│   │   Emulator      │
└─────────────────┘    └──────────────┘    └─────────────────┘
         │                       │                   │
         ▼                       ▼                   ▼
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│  Native App     │    │   Dagster    │    │  Email Alerts   │
│  (Snowflake)    │    │  Pipeline    │    │     (SES)       │
└─────────────────┘    └──────────────┘    └─────────────────┘
```

## Prerequisites

Features in this sample app require a LocalStack for Snowflake license - make sure your Auth Token is configured in your terminal session.

```bash
export LOCALSTACK_AUTH_TOKEN=<your-token>
```

- [Docker](https://docs.docker.com/get-docker/)
- [`localstack` CLI](https://docs.localstack.cloud/getting-started/installation/#localstack-cli).
- [AWS CLI](https://docs.localstack.cloud/user-guide/integrations/aws-cli/) with the [`awslocal` wrapper](https://docs.localstack.cloud/user-guide/integrations/aws-cli/#localstack-aws-cli-awslocal).
- [Snowflake CLI](https://docs.snowflake.com/developer-guide/snowflake-cli/index) with a [`localstack` connection profile](https://snowflake.localstack.cloud/user-guide/integrations/snow-cli/).
- [Python](https://www.python.org/downloads/) 3.10+ & [`pip`](https://pip.pypa.io/en/stable/installation/)
- [`make`](https://www.gnu.org/software/make/) (**optional**, but recommended for running the sample application)

## Installation

To run the sample application, you need to install the required dependencies.

First, clone the repository:

```bash
git clone https://github.com/localstack-samples/snowflake-smart-factory-app.git
```

Then, navigate to the project directory:

```bash
cd snowflake-smart-factory-app
```

Next, install the project dependencies by running the following command:

```bash
make install
```

## Deployment

Start LocalStack with the `LOCALSTACK_AUTH_TOKEN` pre-configured:

```bash
localstack auth set-token <your-auth-token>
DOCKER_FLAGS='-e SF_LOG=trace' \
DEBUG=1 \
IMAGE_NAME=localstack/snowflake:latest \
localstack start
```

To deploy the sample application, run the following command:

```bash
make deploy
```

This will:

- Setup Snowflake database, tables, and Snowpipe
- Setup S3 bucket with event notifications
- Upload sample sensor data (`data/sensor_data_batch_1.csv`)
- Run dbt transformations to process the data

You can also run the following command to deploy the Native app:

```bash
make app
```

The output will be similar to the following:

```bash
App is now accessible at https://snowflake.localhost.localstack.cloud:4566/apps/test/test/FACTORY_APP_HARSHCASPER/
```

The dashboard provides:

- Real-time machine health monitoring
- Interactive sensor data visualizations
- Predictive maintenance recommendations
- Anomaly detection and alerting

## Testing

You can run full end-to-end integration tests using the following command:

```bash
make test
```

This executes:

- **Machine Health Tests**: Verifies health metrics calculations and thresholds
- **Snowpipe Integration Tests**: Tests automated data ingestion workflows

## Use Cases

### Pipeline Orchestration

To run automated dbt transformations on new data, you can launch a Dagster pipeline:

```bash
make pipeline
```

This will:

- Setup a pipeline monitoring dashboard
- Create a S3 sensor to monitor new data
- Trigger dbt transformations on new data

To trigger dbt transformations on new data, you can run the following command:

```bash
make upload LATEST=true
```

This will upload the latest sensor data batch (`data/sensor_data_batch_2.csv`) to the S3 bucket. The Dagster pipeline will automatically detect the new data and trigger the dbt transformations.

To generate new sensor data, you can run the following command:

```bash
make generate
```

### Real-Time Alerting

To set up automated email alerts for critical machine conditions, you can run the following command:

```bash
make alerts
```

This will:

- Create a Python UDF to send email alerts with SES integration
- Create a Snowflake Task to trigger the UDF every 30 seconds
- Create a Snowflake View to query critical machine data
- Send HTML emails for critical machine conditions

In 30 seconds, you can query the SES developer endpoint to see the alert email:

```bash
curl -s http://localhost.localstack.cloud:4566/_aws/ses
```

You can also use the [Mailhog extension](https://github.com/localstack/localstack-extensions/tree/main/mailhog) to view the alert email via a user-friendly interface.

### Data Recovery with Zero-Copy Cloning

To demonstrate instant data recovery capabilities, you can run the following command:

```bash
snow sql -f solutions/data_recovery_clones.sql -c localstack
```

This showcases:

- Instant table cloning without data duplication
- Point-in-time recovery scenarios
- Disaster recovery best practices

## License

This project is licensed under the [Apache License 2.0](LICENSE).
