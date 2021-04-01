from google.cloud import bigquery
from google.oauth2 import service_account


def query(google_credentials: str, sql_query: str) -> None:
    project_id = google_credentials.project_id
    with bigquery.Client(credentials = google_credentials, project = project_id, ) as client:
        try:  # test query
            query_job = client.query(sql_query)
            # TODO make async?
            results = query_job.result()
            return results
        except Exception as e:
            raise e


def export_as_parquet(google_credentials: service_account.Credentials, bigquery_dataset_id: str,
                      bigquery_table_id: str, file_name: str, gcs_export_bucket: str) -> None:
    project_id = google_credentials.project_id
    with bigquery.Client(credentials = google_credentials, project = project_id, ) as client:
        try:
            destination_uri = "gs://{}/{}".format(gcs_export_bucket, file_name)
            dataset_ref = bigquery.DatasetReference(project_id, bigquery_dataset_id)
            table_ref = dataset_ref.table(bigquery_table_id)
            config = bigquery.job.ExtractJobConfig(destination_format = "PARQUET")
            extract_job = client.extract_table(
                table_ref,
                destination_uri,
                # Location must match that of the source table.
                job_config = config,
                location = "EU",
            )  # API request
            extract_job.result()  # Waits for job to complete.
        except Exception as e:
            raise e
