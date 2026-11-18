export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test
export AWS_DEFAULT_REGION=us-east-1
export DBT_WARN_ERROR_OPTIONS={"silence": ["UnusedResourceConfigPath", "NoNodesForSelectionCriteria"]}
SHELL := /bin/bash

# Name `snow app run` gives the Native App: factory_app_<user>, where <user> is $USER
# (or $USERNAME, $LOGNAME) without the characters an unquoted identifier cannot hold.
APP_USER := $(shell printf '%s' "$${USER:-$${USERNAME:-$${LOGNAME:-unknown_user}}}" | tr -cd 'A-Za-z0-9_$$')
APP_NAME ?= FACTORY_APP_$(shell printf '%s' "$(APP_USER)" | tr '[:lower:]' '[:upper:]')
# The app's default Streamlit (CORE.UI) is served at /streamlits/<account>/<app>/<schema>/<name>/;
# the `localstack` connection logs in to account TEST.
APP_URL ?= https://snowflake.localhost.localstack.cloud:4567/streamlits/TEST/$(APP_NAME)/CORE/UI/

# Flag to control which batch file to upload (default: false = use batch 1, true = use latest batch)
LATEST ?= false

check:			## Check if all required prerequisites are installed
	@echo "Checking if all required prerequisites are installed..."
	@which snow > /dev/null 2>&1 || (echo "Snowflake CLI is not installed. Please install it from https://docs.snowflake.com/en/user-guide/snowsql-install-config.html" && exit 1)
	@docker compose version > /dev/null 2>&1 || (echo "Docker Compose is not installed. Please install it from https://docs.docker.com/compose/install/" && exit 1)
	@echo python > /dev/null 2>&1 || (echo "Python is not installed. Please install it from https://www.python.org/downloads/" && exit 1)
	@echo virtualenv > /dev/null 2>&1 || (echo "virtualenv is not installed. Please install it from https://virtualenv.pypa.io/en/latest/installation.html" && exit 1)
	@echo docker > /dev/null 2>&1 || (echo "Docker is not installed. Please install it from https://docs.docker.com/get-docker/" && exit 1)
	@echo "All prerequisites are installed."

usage:			## Show this help in table format
	@echo "| Target                 | Description                                                       |"
	@echo "|------------------------|-------------------------------------------------------------------|"
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/:.*##\s*/##/g' | awk -F'##' '{ printf "| %-22s | %-65s |\n", $$1, $$2 }'

install:		## Install dependencies
	@echo "Installing dependencies..."
	pip install virtualenv
	virtualenv env
	bash -c "source env/bin/activate && pip install -r requirements.txt"
	bash -c "source env/bin/activate && dbt deps"
	@echo "Dependencies installed successfully."

seed:			## Create & Seed the database
	@echo "Seeding the database..."
	snow sql -f setup/01_setup_snowflake.sql -c localstack
	@echo "Database seeded successfully."

aws:			## Setup AWS resources
	@echo "Setting up AWS resources..."
	bash -c "source env/bin/activate && python setup/02_configure_s3_bucket.py"
	@echo "AWS resources setup successfully."

generate:		## Generate new sensor data batch
	@echo "Generating new sensor data batch..."
	bash -c "source env/bin/activate && python bin/data_generator.py"
	@echo "New sensor data batch generated successfully."

upload:			## Upload files to S3 (use LATEST=true to upload highest batch number)
	@echo "Uploading files to S3..."
ifeq ($(LATEST),true)
	bash -c "source env/bin/activate && python setup/03_upload_file.py --latest"
else
	bash -c "source env/bin/activate && python setup/03_upload_file.py"
endif
	@echo "Files uploaded successfully."

pipeline:		## Setup Dagster pipeline
	@echo "Setting up pipeline..."
	bash -c "source env/bin/activate && dagster dev -f pipeline/assets.py"
	@echo "Pipeline setup successfully."

dbt:			## Run dbt models
	@echo "Running dbt..."
	bash -c "source env/bin/activate && dbt run --profile localstack"
	@echo "Dbt run successfully."

app:			## Run Snowflake Native App
	@echo "Running app..."
	cd app && snow app run -c localstack --role ACCOUNTADMIN
	snow sql -f app/grants.sql -c localstack --role ACCOUNTADMIN -D app_name=$(APP_NAME)
	@echo ""
	@echo "[●] Deployed on LocalStack"
	@printf " └─ URL: \033[4;94m$(APP_URL)\033[0m\n"
	@echo ""

app-check:		## Check that the Native App dashboard is served
	@echo "Checking the Native App dashboard at $(APP_URL)..."
	bash -c "source env/bin/activate && python bin/check_app.py $(APP_URL)"

streamlit:		## Run Streamlit app
	@echo "Running Streamlit app..."
	cd app/src/module-ui/src && streamlit run ui.py
	@echo "Streamlit app run successfully."

deploy:			## Deploy the entire stack
	@echo "Deploying the entire stack..."
	make seed
	make aws
	make upload
	make dbt
	@echo "Deployment completed successfully. Run 'make app' to view the app."

debug:			## Clean up everything and redeploy to fix bugs
	@echo "Running debug cleanup and redeploy..."
	snow sql -c localstack -q "USE DATABASE FACTORY_PIPELINE_DEMO; USE SCHEMA PUBLIC; DROP FILE FORMAT IF EXISTS csv_format;"
	snow sql -c localstack -q "DROP PIPE IF EXISTS FACTORY_PIPELINE_DEMO.PUBLIC.SENSOR_DATA_PIPE;"
	snow sql -c localstack -q "DROP TABLE IF EXISTS FACTORY_PIPELINE_DEMO.PUBLIC_RAW.SENSOR_DATA;"
	awslocal s3 rb s3://factory-sensor-data-local --force
	make deploy
	@echo "Debug cleanup and redeploy completed successfully."

alerts:			## Setup alerts
	@echo "Setting up alerts..."
	awslocal ses verify-email-identity --email hello@example.com
	snow sql -f solutions/task_alert_udf.sql -c localstack
	@echo "Alerts setup successfully."

test:			## Run tests
	@echo "Running tests..."
	@echo "Running dbt tests..."
	bash -c "source env/bin/activate && dbt test --profile localstack"
	@echo "Running pytest integration tests..."
	bash -c "source env/bin/activate && pytest tests/"
	@echo "Tests run successfully."

start:			## Start LocalStack for AWS and the Snowflake emulator (Snowflake Next)
	@echo "Starting LocalStack..."
	@test -n "${LOCALSTACK_AUTH_TOKEN}" || (echo "LOCALSTACK_AUTH_TOKEN is not set. Find your token at https://app.localstack.cloud/workspace/auth-token"; exit 1)
	docker compose up --detach --wait
	@echo "LocalStack started successfully."

stop:			## Stop LocalStack
	@echo "Stopping LocalStack..."
	@docker compose down --volumes
	@echo "LocalStack stopped successfully."

ready:			## Make sure the LocalStack containers are up
		@echo Waiting on the LocalStack containers...
		@docker compose up --detach --wait > /dev/null && echo LocalStack is ready to use! || (echo Gave up waiting on LocalStack, exiting. && exit 1)

logs:			## Save the logs in a separate file
		@docker compose logs > logs.txt

.PHONY: install seed aws upload pipeline dbt app app-check deploy test start stop ready logs debug generate
