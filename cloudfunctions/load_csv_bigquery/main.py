 # Copyright (C) 2024 Google Inc.
 #
 # Licensed under the Apache License, Version 2.0 (the "License"); you may not
 # use this file except in compliance with the License. You may obtain a copy of
 # the License at
 #
 # http://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing, software
 # distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 # WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 # License for the specific language governing permissions and limitations under
 # the License.

from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pathlib import Path
from google.cloud import storage

import functions_framework
import sys
import os
import re
import base64
import json
import argparse

# GCP clients
bq_client = bigquery.Client()
storage_client = storage.Client()
# date extraction regex
date_regex = re.compile(r"(\d{4}-\d{2}-\d{2}|\d{4}\d{2}\d{2}|\d{4}-\d{2}|\d{4}\d{2})")


def list_objects_bucket(bucket, prefix_path):
    return list(map(lambda b: b.name,  storage_client.list_blobs(bucket, prefix=prefix_path)))


@functions_framework.cloud_event
def gcs_object_listener(cloud_event: CloudEvent) -> tuple:
    data = json.loads(base64.b64decode(cloud_event.data['message']['data']))

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]
    content_type = data["contentType"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    print(f"Content type: {content_type}")

    if content_type == "text/csv":
        table_id, partition = extract_table_id_and_partition(name)
        load_csv_into_bq(table_id, partition, bucket, name)

    return event_id, event_type, bucket, name, metageneration, timeCreated, updated


def extract_partition_from_name(file_name: str) -> str:
    match = date_regex.match(file_name)
    if match:
        date = match.group().replace("-", "")
        return "DAY" if len(date) == 8 else "MONTH"
    return "NA"


def extract_table_id_and_partition(file_name: str, project="", dataset="") -> (str, str):
    name = Path(file_name).stem
    no_date_name = date_regex.sub("", name)
    no_whitespace_name = '_'.join(no_date_name.split())
    lowercase_name = no_whitespace_name.lower()
    project_id = os.getenv("GCP_PROJECT", project)
    dataset = os.getenv("DATASET", dataset)
    table_id = f"{project_id}.{dataset}.{lowercase_name}"
    return (table_id, extract_partition_from_name(name))


def time_partitioning(partition: str):
    if partition == "DAY":
        return bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        )
    if partition == "MONTH":
        return None
    return None


def load_csv_into_bq(table_id, partition, bucket_name, file_path):

    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        schema_update_options=[
            "ALLOW_FIELD_ADDITION",
            "ALLOW_FIELD_RELAXATION"
        ],
        autodetect=True,
        skip_leading_rows=1,
    )
    uri = f"gs://{bucket_name}/{file_path}"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Wait for the job to complete.

    table = bq_client.get_table(table_id)
    print("Loaded data to table {}, num rows {}".format(table_id, table.num_rows))


def load_gcs_file(bucket_name, file_name, project="", dataset=""):
    table_id, partition = extract_table_id_and_partition(file_name, project, dataset)
    print(f"will upload data to table: {table_id} with partition {partition}")
    load_csv_into_bq(table_id, partition, bucket_name, file_name)


def load_gcs_files(bucket, project="", dataset="", root_path=""):
    files = list_objects_bucket(bucket, root_path)
    for file in files:
        load_gcs_file(bucket, file, project, dataset)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load CSV file into BQ')
    parser.add_argument('--project', required=True,
                    help='gcp project for bq.')
    parser.add_argument('--dataset', required=True,
                    help='a bq dataset.')
    parser.add_argument('--bucket', required=True,
                    help='a gcs bucket.')

    args = parser.parse_args()

    load_gcs_files(args.bucket, project=args.project, dataset=args.dataset)