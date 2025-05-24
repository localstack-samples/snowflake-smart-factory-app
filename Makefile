export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test
export AWS_DEFAULT_REGION=us-east-1
SHELL := /bin/bash


check:			## Check if all required prerequisites are installed
	@echo "Checking if all required prerequisites are installed..."
	@which snow > /dev/null 2>&1 || (echo "Snowflake CLI is not installed. Please install it from https://docs.snowflake.com/en/user-guide/snowsql-install-config.html" && exit 1)
	@which localstack > /dev/null 2>&1 || (echo "LocalStack is not installed. Please install it from https://docs.localstack.cloud/getting-started/installation/" && exit 1)
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

upload:			## Upload files to S3
	@echo "Uploading files to S3..."
	bash -c "source env/bin/activate && python setup/03_upload_file.py"
	@echo "Files uploaded successfully."

pipeline:		## Setup Dagster pipeline
	@echo "Setting up pipeline..."
	bash -c "source env/bin/activate && dagster dev -f pipeline/assets.py"
	@echo "Pipeline setup successfully."

dbt:			## Run dbt models
	@echo "Running dbt..."
	bash -c "source env/bin/activate && dbt build --profile localstack"
	@echo "Dbt run successfully."

app:			## Run Snowflake Native App
	@echo "Running app..."
	cd app && snow app run -c localstack
	@echo "App is now accessible at https://snowflake.localhost.localstack.cloud:4566/apps/test/test/FACTORY_APP_HARSHCASPER/"

deploy:			## Deploy the entire stack
	@echo "Deploying the entire stack..."
	make seed
	make aws
	make upload
	make dbt
	@echo "Deployment completed successfully. Run 'make app' to view the app."

test:			## Run tests
	@echo "Running tests..."
	bash -c "source env/bin/activate && pytest tests/"
	@echo "Tests run successfully."

start:			## Start LocalStack
	@echo "Starting LocalStack..."
	DOCKER_FLAGS='-e SF_LOG=trace' \
	DEBUG=1 \
	IMAGE_NAME=localstack/snowflake:latest \
	LOCALSTACK_AUTH_TOKEN=$(LOCALSTACK_AUTH_TOKEN) \
	localstack start -d
	@echo "LocalStack started successfully."

stop:			## Stop LocalStack
	@echo "Stopping LocalStack..."
	@localstack stop
	@echo "LocalStack stopped successfully."

ready:			## Make sure the LocalStack container is up
		@echo Waiting on the LocalStack container...
		@localstack wait -t 30 && echo LocalStack is ready to use! || (echo Gave up waiting on LocalStack, exiting. && exit 1)

logs:			## Save the logs in a separate file
		@localstack logs > logs.txt

.PHONY: install seed aws upload pipeline dbt app deploy test start stop ready logs
