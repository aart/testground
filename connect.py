from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
import json
import googleapiclient.discovery

key_path = "testground-97-13593ff4ef64.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

project_id=credentials.project_id
dataset_id = "test"
bucket_name = "testground-97"
table_id = "housing"


# pipeline step 1

with bigquery.Client(credentials=credentials, project=project_id,) as client:

    destination_uri = "gs://{}/{}".format(bucket_name, "housing.parquet")
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)

    query_job = client.query(
        """
        SELECT
          CONCAT(
            'https://stackoverflow.com/questions/',
            CAST(id as STRING)) as url,
          view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 10"""
    )
    try: # test query
        results = query_job.result()
    except Exception as e:
        print(e)

    try: # test export
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
    except Exception as e:
        print(e)


# pipeline step 2

description = "groundtest file transfer"
start_date = datetime.date(2021, 3, 29)
start_time = datetime.time(hour=20)
sink_bucket = "azure-proxy"
source_bucket = bucket_name

with googleapiclient.discovery.build('storagetransfer', 'v1',credentials=credentials) as storagetransfer:

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
    try:
        result = storagetransfer.transferJobs().create(body=transfer_job).execute()
        print('Returned transferJob: {}'.format(
            json.dumps(result, indent=4)))
    except Exception as e:
        print("error")
        print(e)
