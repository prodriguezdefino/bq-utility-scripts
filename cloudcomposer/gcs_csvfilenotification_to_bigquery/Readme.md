# Daily CSV Upload to BigQuery

This folder contains the scripts needed to create a Cloud Composer environment and deploy an Airflow DAG in charge of reading files uploaded into a GCS bucket and upload them into BigQuery creating the tables if necesssary.

## Setup

The included DAG (file `reports_bq_uploader.py`) makes use of Airflow variables to configure which project and dataset the BigQuery destination will be used, the bucket where the reports are and a service account with enough permissions to read from the bucket and to create and write the BigQuery tables. As the first step run `cp variables.json variables__local.json` to copy the example file and setup the right values for the execution.

Run the `create_environment.sh` script to create the Cloud Composer environment and setup the variables right after the creation is completed.

Once this is done by running `sh upload_dag.sh` the DAG script will be deployed.