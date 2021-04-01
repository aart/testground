from google.cloud import bigquery
import unittest
from googledataload import cloud_connections

class TestModuleFunctions(unittest.TestCase):
    def test_public_query(self):
        print('TODO')

def query( google_credentials ,sql_query):
    project_id = google_credentials.project_id
    with bigquery.Client(credentials=google_credentials, project=project_id,) as client:
        try: # test query
            query_job = client.query(sql_query)
            results = query_job.result()
            return results
        except Exception as e:
            return e

def query_and_export(key_path, sql_query):
    google_credentials = cloud_connections.initialize_google_account_from_file(key_path)
    project_id = credentials.project_id

    # configuration TODO
    bigquery_dataset_id="test"
    bigquery_table_id="housing"
    gcs_origin_bucket="testground-97"
    lake_destination_bucket="azure-proxy"

    with bigquery.Client(credentials= google_credentials, project=project_id,) as client:

        try:
            result = query(google_credentials, sql_query)

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
                 return e
        except Exception as e:
            return e


if '__name__'=='__main__':
    unittest.main()