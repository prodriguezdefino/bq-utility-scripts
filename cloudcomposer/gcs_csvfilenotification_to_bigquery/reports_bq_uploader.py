import datetime
import re

from airflow import DAG
from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.operators.python import get_current_context
from google.cloud import bigquery, storage
from pathlib import Path
from typing import List



# date extraction regex
date_regex = re.compile(r"(\d{4}-\d{2}-\d{2}|\d{4}\d{2}\d{2}|\d{4}-\d{2}|\d{4}\d{2})")


# creates a Credentials object impersonating the provided service account and scopes
def create_impersonated_credentials(
    impersonated_service_account: str,
    scope: str = "cloud-platform"):
    from google.auth import impersonated_credentials
    import google.auth

    credentials, project_id = google.auth.default()

    target_credentials = impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=impersonated_service_account,
        delegates=[],
        target_scopes=[scope],
        lifetime=300,
    )


def should_process(run_date: str, file_date: str) -> bool:
    print(f'comparison {run_date} and {file_date}')
    return run_date == file_date


def extract_date_from_filename(file_name: str) -> str:
    match = date_regex.match(file_name)
    if match:
        date_str = match.group().replace("-", "")
        if len(date_str) == 8:
            return date_str
        return date_str + "01"
    return "NA"


def list_objects_bucket(
        run_date: str,
        bucket: str,
        prefix_path: str,
        service_account: str) -> List[str]:
    storage_client = storage.Client(credentials=create_impersonated_credentials(service_account))
    return list(
                filter(
                    lambda d: should_process(run_date, extract_date_from_filename(d)),
                    map(
                        lambda b: b.name,
                        storage_client.list_blobs(bucket, prefix=prefix_path))))


def extract_table_id(file_name: str, project: str, dataset: str) -> str:
    name = Path(file_name).stem
    no_date_name = date_regex.sub("", name)
    no_whitespace_name = '_'.join(no_date_name.split())
    lowercase_name = no_whitespace_name.lower()
    table_id = f"{project}.{dataset}.{lowercase_name}"
    return table_id


def load_csv_into_bq(table_id: str,  bucket_name: str, file_path: str, bq_client):
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


@dag(
    schedule="@daily",
    start_date=datetime.datetime(2024, 3, 1),
    catchup=False)
def bq_upload_reports():

    @task()
    def discover_new_files(bucket, service_account) -> List[str]:
        context = get_current_context()
        run_date = context['ds_nodash']
        return list_objects_bucket(run_date, bucket, None, service_account)

    @task(depends_on_past=True)
    def upload_files(files: List[str], project, dataset, bucket_name, service_account):
        bq_client = None
        for file_name in files:
            bq_client = (
                bq_client if bq_client
                else bigquery.Client(credentials=create_impersonated_credentials(service_account)))
            table_id = extract_table_id(file_name, project, dataset)
            print(f"will upload data to table: {table_id}")
            load_csv_into_bq(table_id, bucket_name, file_name, bq_client)

    service_account = Variable.get('service_account')
    upload_files(
        discover_new_files(Variable.get('bucket'), service_account),
        Variable.get('project'),
        Variable.get('dataset'),
        Variable.get('bucket'),
        service_account)


bq_upload_reports()