#!/bin/bash
set -eux

COMPOSER_ENV=reports-upload-bq-test
REGION=us-central1

gcloud composer environments storage dags import \
    --environment $COMPOSER_ENV  --location $REGION \
    --source reports_bq_uploader.py