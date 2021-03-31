from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
import json
import googleapiclient.discovery
import unittest
from azure.storage.filedatalake import DataLakeServiceClient


# test configuration TODO
key_path = "./testground-97-13593ff4ef64.json"
bigquery_dataset_id = "test"
bigquery_table_id = "housing"
gcs_origin_bucket = "testground-97"
lake_destination_bucket = "azure-proxy"
sql_query = """
    SELECT
      CONCAT(
        'https://stackoverflow.com/questions/',
        CAST(id as STRING)) as url,
      view_count
    FROM `bigquery-public-data.stackoverflow.posts_questions`
    WHERE tags like '%google-bigquery%'
    ORDER BY view_count DESC
    LIMIT 10"""
transfer_description = "groundtest file transfer"
transfer_start_date = datetime.date(2021, 3, 30)
transfer_start_time = datetime.time(hour=20)

AZURE_KEY= "cLgpT3gjRAXj5fyIHVot23V/+EDj0TIPzDu7Z78pYmpFZGCjfb8sk/xgGnA8hfbv0aEBig0j64J5+TWYfqHohw=="
azure_storage_account = ""
azure_container = ""

# initialize
def initialize_google_account(key_path):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credentials

def initialize_azure_account(storage_account_name, storage_account_key):
    try:
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

    except Exception as e:
        print("azure error handling")  # TODO
        print(e)

def create_azure_directory():
    initialize_azure_account("googledata", AZURE_KEY)

    try:
        global file_system_client
        file_system_client = service_client.create_file_system(file_system="my-file-system")
        try:
            file_system_client.create_directory("my-directory")
        except Exception as e:
            print(e)
    except Exception as e:
        print(e)

# pipeline step 1
def step_1_query_and_export():
    credentials = initialize_google_account(key_path)
    project_id = credentials.project_id

    with bigquery.Client(credentials=credentials, project=project_id,) as client:

        try: # test query
            query_job = client.query(sql_query)
            results = query_job.result()
            print("Queried")
            try:  # test export
                destination_uri = "gs://{}/{}".format(gcs_origin_bucket, "housing.parquet")
                dataset_ref = bigquery.DatasetReference(project_id, bigquery_dataset_id)
                table_ref = dataset_ref.table(bigquery_table_id)
                config = bigquery.job.ExtractJobConfig(destination_format="PARQUET")
                extract_job = client.extract_table(
                    table_ref,
                    destination_uri,
                    # Location must match that of the source table.
                    job_config=config,
                    location="EU",
                )  # API request
                extract_job.result()  # Waits for job to complete.
                print(
                    "Exported {}:{}.{} to {}".format(project_id, bigquery_dataset_id, bigquery_table_id, destination_uri)
                )
            except Exception as e:
                print("export error handling")  # TODO
                print(e)
        except Exception as e:
            print("query error handling") #TODO
            print(e)


# pipeline step 2
def step_2a_transfer_to_lake():
    credentials = initialize_google_account(key_path)
    project_id = credentials.project_id
    with googleapiclient.discovery.build('storagetransfer', 'v1',credentials=credentials) as storagetransfer:

        # Edit this template with desired parameters.
        transfer_job = {
            'description': transfer_description,
            'status': 'ENABLED',
            'projectId': project_id,
            'schedule': {
                'scheduleStartDate': {
                    'day': transfer_start_date.day,
                    'month': transfer_start_date.month,
                    'year': transfer_start_date.year
                },
                'scheduleEndDate': {
                    'day': transfer_start_date.day,
                    'month': transfer_start_date.month,
                    'year': transfer_start_date.year
                },
                'startTimeOfDay': {
                    'hours': transfer_start_time.hour,
                    'minutes': transfer_start_time.minute,
                    'seconds': transfer_start_time.second
                }
            },
            'transferSpec': {
                'gcsDataSource': {
                    'bucketName': gcs_origin_bucket
                },
                'gcsDataSink': {
                    'bucketName': lake_destination_bucket
                }
            }
        }
        try:
            result = storagetransfer.transferJobs().create(body=transfer_job).execute()
            print('Returned transferJob: {}'.format(
                json.dumps(result, indent=4)))
        except Exception as e:
            print("transfer error handling") #TODO
            print(e)

# pipeline step 2
def step_2b_transfer_to_lake():
    credentials = initialize_google_account(key_path)
    project_id = credentials.project_id
    with googleapiclient.discovery.build('storagetransfer', 'v1',credentials=credentials) as storagetransfer:

        # Edit this template with desired parameters.
        transfer_job = {
            'description': transfer_description,
            'status': 'ENABLED',
            'projectId': project_id,
            'schedule': {
                'scheduleStartDate': {
                    'day': transfer_start_date.day,
                    'month': transfer_start_date.month,
                    'year': transfer_start_date.year
                },
                'scheduleEndDate': {
                    'day': transfer_start_date.day,
                    'month': transfer_start_date.month,
                    'year': transfer_start_date.year
                },
                'startTimeOfDay': {
                    'hours': transfer_start_time.hour,
                    'minutes': transfer_start_time.minute,
                    'seconds': transfer_start_time.second
                }
            },
            'transferSpec': {
                'gcsDataSource': {
                    'bucketName': gcs_origin_bucket
                },
                'azureBlobStorageDataSink': {
                    'storageAccount': 'googledata',
                    'azureCredentials': {
                        'sasToken': AZURE_KEY,
                    },
                    'container': 'my-file-system',
                }
            }
        }
        try:
            result = storagetransfer.transferJobs().create(body=transfer_job).execute()
            print('Returned transferJob: {}'.format(
                json.dumps(result, indent=4)))
        except Exception as e:
            print("transfer error handling") #TODO
            print(e)

