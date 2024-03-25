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
from google.auth import default

import functions_framework
import sys
import os
import re

_, project_id = default()

def list_objects_bucket(bucket, prefix_path):
    client = storage.Client()
    return list(map(lambda b: b.name,  client.list_blobs(bucket, prefix=prefix_path)))


@functions_framework.cloud_event
def gcs_object_listener(cloud_event: CloudEvent) -> tuple:
    data = cloud_event.data

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
    print(f"Content type: {updated}")

    if content_type == "text/csv":
        load_csv_into_bq(extract_table_id(name), bucket, name)

    return event_id, event_type, bucket, name, metageneration, timeCreated, updated


def extract_table_id(file_name: str) -> str:
    name = Path(file_name).stem
    no_date_name = re.sub(r"\d{4}-\d{2}-\d{2}","", name)
    no_whitespace_name = '_'.join(no_date_name.split())
    lowercase_name = no_whitespace_name.lower()
    dataset = "vertex_usage"
    table_id = f"{project_id}.{dataset}.{lowercase_name}"
    return table_id


def load_csv_into_bq(table_id, bucket_name, file_path):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        schema_update_options=[
            "ALLOW_FIELD_ADDITION",
            "ALLOW_FIELD_RELAXATION"
        ],
        autodetect=True,
        skip_leading_rows=1,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        ),
    )
    uri = f"gs://{bucket_name}/{file_path}"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)
    print("Loaded data to table {}, num rows {}".format(table_id, table.num_rows))


def load_gcs_file(bucket_name, file_name):
    table_id = extract_table_id(file_name)
    load_csv_into_bq(table_id, bucket_name, file_name)


def load_gcs_files(bucket, root_path=""):
    files = list_objects_bucket(bucket, root_path)
    for file in files:
        load_gcs_file(bucket, file)

if __name__ == '__main__':
    load_gcs_files(sys.argv[1])