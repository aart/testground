import datetime
import json
import googleapiclient.discovery

# configuration TODO
key_path = "../google_key.json"
gcs_origin_bucket = "testground-97"
lake_destination_bucket = "azure-proxy"
transfer_description = "groundtest file transfer"
transfer_start_date = datetime.date(2021, 3, 30)
transfer_start_time = datetime.time(hour=20)
azure_storage_account = ""
azure_container = ""

# pipeline step 2

def step_2a_transfer_to_lake():
    credentials = connect.initialize_google_account(key_path)
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
    credentials = connect.initialize_google_account(key_path)
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
                        'sasToken': azure_key,
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

