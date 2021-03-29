from google.cloud import bigquery
from google.oauth2 import service_account
import argparse
import datetime
import json
import googleapiclient.discovery

key_path = "testground-97-13593ff4ef64.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

project_id=credentials.project_id

client = bigquery.Client(credentials=credentials, project=project_id,)

dataset_id = "test"
bucket_name = "testground-97"
table_id = "housing"

destination_uri = "gs://{}/{}".format(bucket_name, "housing.parquet")
dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
table_ref = dataset_ref.table(table_id)

extract_job = client.extract_table(
    table_ref,
    destination_uri,
    # Location must match that of the source table.
    location="EU",
)  # API request
extract_job.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(project_id, dataset_id, table_id, destination_uri)
)


def transfer(description, project_id, start_date, start_time, source_bucket, sink_bucket):
    """Create a one-time transfer from Amazon S3 to Google Cloud Storage."""
    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

    # Edit this template with desired parameters.
    transfer_job = {
        'description': description,
        'status': 'ENABLED',
        'projectId': project_id,
        'schedule': {
            'scheduleStartDate': {
                'day': start_date.day,
                'month': start_date.month,
                'year': start_date.year
            },
            'scheduleEndDate': {
                'day': start_date.day,
                'month': start_date.month,
                'year': start_date.year
            },
            'startTimeOfDay': {
                'hours': start_time.hour,
                'minutes': start_time.minute,
                'seconds': start_time.second
            }
        },
        'transferSpec': {
            'gcsDataSource': {
                'bucketName': source_bucket
            },
            'gcsDataSink': {
                'bucketName': sink_bucket
            }
        }
    }

    result = storagetransfer.transferJobs().create(body=transfer_job).execute()
    print('Returned transferJob: {}'.format(
        json.dumps(result, indent=4)))



description = "groundtest"
start_date = "2021-03-29"
start_time= "00:00:00"
sink_bucket = "azure-proxy"

transfer(description, project_id, start_date, start_time, bucket_name, sink_bucket)