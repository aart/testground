from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
import json
import googleapiclient.discovery


# test configuration
key_path = "testground-97-13593ff4ef64.json"
dataset_id = "test"
table_id = "housing"
source_bucket = "testground-97"
sink_bucket = "azure-proxy"
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

# initialize
def get_credentials_from_key_file(key_path):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credentials


credentials = get_credentials_from_key_file(key_path)
project_id= credentials.project_id


# pipeline step 1
def step_1_query_and_export():
    with bigquery.Client(credentials=credentials, project=project_id,) as client:

        try: # test query
            query_job = client.query(sql_query)
            results = query_job.result()
            print("Queried")
            try:  # test export
                destination_uri = "gs://{}/{}".format(source_bucket, "housing.parquet")
                dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
                table_ref = dataset_ref.table(table_id)
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
                    "Exported {}:{}.{} to {}".format(project_id, dataset_id, table_id, destination_uri)
                )
            except Exception as e:
                print("export error handling")  # TODO
                print(e)
        except Exception as e:
            print("query error handling") #TODO
            print(e)


# pipeline step 2
def step_2_transfer_to_lake():
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
            print("transfer error handling") #TODO
            print(e)


step_1_query_and_export()
step_2_transfer_to_lake()