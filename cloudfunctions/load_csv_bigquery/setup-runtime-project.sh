#!/bin/bash
set -eux

if [ "$#" -ne 5 ]
  then
    echo "Usage : sh setup-runtime-project.sh <runtime project id> <data project id> <pubsub topic> <bq dataset> <service account email>"
    exit -1
fi

TOPIC=$3
DATASET=$4
CF_NAME=load-vertex-usage-reports
REGION=us-central1
PROJECT_ID=$1
DATA_PROJECT_ID=$2
SA_EMAIL=$5

#create cloud function
gcloud functions deploy $CF_NAME \
    --project=$PROJECT_ID \
    --gen2 \
    --runtime=python312 \
    --region=$REGION \
    --source=. \
    --set-env-vars "GCP_PROJECT=${DATA_PROJECT_ID},DATASET=${DATASET}" \
    --retry \
    --service-account=$SA_EMAIL \
    --entry-point=gcs_object_listener \
    --trigger-topic=$TOPIC

# add bq job user permission
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.jobUser"

# add cloud run invoker permission
gcloud run services add-iam-policy-binding $CF_NAME \
    --project=$PROJECT_ID \
    --region=$REGION \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/run.invoker"

