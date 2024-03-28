# Cross project event driven ingestion on BigQuery

This repository provides an example on how to setup a GCP CloudFunction which capture object finalized events from GCS produced on a different project and loads the data from such objects (as CSV files) into a BigQuery table (creating the table and schema on demand if needed).

To simplify the understanding we will be referring to the projects as `data` and `runtime` projects. The `data` project is where there the GCS bucket and the BigQuery dataset exists, and `runtime` project is where we will be setting up resources to execute the event processing.

## Pre setup

Before executing the scripts included as part of the setup for the solution we need to make sure we have some requisites fulfilled first:
* Enable PubSub service on `runtime` project
* Enable CloudFunctions service on `runtime` project
* Enable CloudRun service on `runtime` project (in as the runtime for the cloud function)
* Enable CloudBuild service on `runtime` project (in use as the build facility for the cloud function code)
* Enable Eventarc service on `runtime` project.
* Create a bucket in the `data` project
* Create a bigquery dataset in the `data` project
* Create a service account on the `runtime` project
* create a PubSub topic on the `runtime` project
* Grant read permissions (Storage Object Viewer) to this service account for the bucket located in the `data` project
* Grant write permissions (Data Editor) to this service account for the bigquery dataset in the `data` project

Also, there should be a local installation of `gcloud` with the right permissions on both projects.

Permissions needed for the `data` project:
* Project IAM Admin, to be able to provide the permissions to the service accounts as part of the scripts
* Storage Admin, to setup the notifications at the bucket level

Permissions needed for the `runtime` project:
* Project IAM Admin, to be able to provide the permissions to the service accounts as part of the scripts
* Service Usage Admin, to enable the required services
* Pub/Sub Admin, to create the topic
* Cloud Functions Admin, to be able to deploy cloud functions
* Project IAM Admin

## Setup execution

Once the prerequisites are completed we can execute the included scripts to setup the permissions and wirings on both projects.
* run `setup-data-project.sh` script
* run `setup-runtime-project.sh` script

Finally, by adding a CSV file in the bucket created on the `data` project we should see, after a few seconds, the creation of the corresponding table in the expected BigQuery dataset containing the file's content as data.


## Ingesting pre-existing data into BigQuery

This setup will take care of quickly ingest new files that are added to the configured bucket. In case of needing to load data from files pre-existing on the bucket by setting up environment variables `GCP_PROJECT` and `DATASET` corresponding to the destination BigQuery resources and running `python3 main.py <bucket name>` the script will list the files on the bucket and ingest them one by one into BigQuery.

### Disclaimer
This software is provided as-is, without warranty or representation for any use or purpose.