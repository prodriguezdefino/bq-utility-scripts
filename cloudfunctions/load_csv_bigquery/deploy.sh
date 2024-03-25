#!/bin/bash
set -eux

BUCKET_NAME=$1
CF_NAME=load-vertex-usage-reports
REGION=us-central1
PROJECT_ID=$(gcloud config get-value project)
PROJECT_NUMBER=$(gcloud projects list --filter="project_id:$PROJECT_ID" --format='value(project_number)')

# grab gcs service account for the project
SERVICE_ACCOUNT=$(gsutil kms serviceaccount -p $PROJECT_NUMBER)

# make sure it has permissions to publish events to pubsub
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member serviceAccount:$SERVICE_ACCOUNT \
  --role roles/pubsub.publisher

# and JIC pubsub sa do have permissions to create tokens for SAs
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountTokenCreator"

# service account for cloudfunction
SA_NAME="vertex-usage-report-uploader"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# add event arc receiver permission
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/eventarc.eventReceiver"

# add bq job user permission
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.jobUser"

#create cloud function
gcloud functions deploy $CF_NAME \
    --project=$PROJECT_ID \
    --gen2 \
    --runtime=python312 \
    --region=$REGION \
    --source=. \
    --retry \
    --service-account=$SA_EMAIL \
    --entry-point=gcs_object_listener \
    --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
    --trigger-event-filters="bucket=${BUCKET_NAME}"

# add cloud run invoker permission
gcloud run services add-iam-policy-binding $CF_NAME \
    --project=$PROJECT_ID \
    --region=$REGION \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/run.invoker"