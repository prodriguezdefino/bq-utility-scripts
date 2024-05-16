#!/bin/bash
set -eux

if [ "$#" -ne 1 ]
  then
    echo "Usage : sh create_environment.sh <project id>"
    exit -1
fi

PROJECT_ID=$1
PROJECT_NUMBER=$(gcloud projects list --filter="project_id:$PROJECT_ID" --format='value(project_number)')
REGION=us-central1
COMPOSER_ENV=reports-upload-bq-test

# Add the Cloud Composer v2 API Service Agent Extension role
gcloud iam service-accounts add-iam-policy-binding \
    $PROJECT_NUMBER-compute@developer.gserviceaccount.com \
    --member serviceAccount:service-$PROJECT_NUMBER@cloudcomposer-accounts.iam.gserviceaccount.com \
    --role roles/composer.ServiceAgentV2Ext

gcloud composer environments create $COMPOSER_ENV \
    --project $PROJECT_ID \
    --location $REGION \
    --image-version composer-2.7.1-airflow-2.7.3

gcloud beta composer environments storage data import --environment=$COMPOSER_ENV --location=$REGION \
  --source=variables__local.json --destination=/variables

gcloud composer environments run $COMPOSER_ENV --location=$REGION variables -- import /home/airflow/gcs/data/variables/variables__local.json
