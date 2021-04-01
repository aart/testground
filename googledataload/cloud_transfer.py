import googleapiclient.discovery
from google.oauth2 import service_account

# pipeline step 2


# TODO add type annotations
def transfer_to_lake(google_credentials : service_account.Credentials,gcs_origin_bucket, destination_bucket,transfer_start_date,transfer_start_time):
    project_id = google_credentials.project_id
    with googleapiclient.discovery.build('storagetransfer', 'v1',credentials=google_credentials) as storagetransfer:

        # Edit this template with desired parameters.
        transfer_job = {
            'description': "file transfer to azure data lake",
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
