#!/bin/bash
set -eux

if [ "$#" -ne 4 ]
  then
    echo "Usage : sh setup-data-project.sh <data project id> <runtime project id> <gcp bucket> <pubsub topic>"
    exit -1
fi

BUCKET_NAME=$3
TOPIC=$4
DATA_PROJECT_ID=$1
DATA_PROJECT_NUMBER=$(gcloud projects list --filter="project_id:$DATA_PROJECT_ID" --format='value(project_number)')
RUNTIME_PROJECT_ID=$2

# grab gcs service account for the project
SERVICE_ACCOUNT=$(gsutil kms serviceaccount -p $DATA_PROJECT_NUMBER)

# make sure it has permissions to publish events to pubsub
gcloud projects add-iam-policy-binding $RUNTIME_PROJECT_ID \
  --member serviceAccount:$SERVICE_ACCOUNT \
  --role roles/pubsub.publisher

# and JIC pubsub sa do have permissions to create tokens for pubsub agent SA
gcloud projects add-iam-policy-binding $DATA_PROJECT_ID \
    --member="serviceAccount:service-${DATA_PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountTokenCreator"

# create the object event notification for the topic
gcloud storage buckets notifications create "gs://${BUCKET_NAME}" --topic=$TOPIC --event-types=OBJECT_FINALIZE