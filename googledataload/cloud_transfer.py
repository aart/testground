import datetime

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

def transfer_to_lake(google_credentials,gcs_origin_bucket, destination_bucket):
    project_id = google_credentials.project_id
    with googleapiclient.discovery.build('storagetransfer', 'v1',credentials=google_credentials) as storagetransfer:

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
                    'bucketName': destination_bucket
                }
            }
        }
        try:
            job = storagetransfer.transferJobs().create(body=transfer_job).execute()
            return job

        except Exception as e:
            raise e
