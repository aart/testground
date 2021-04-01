from google.cloud import bigquery

# configuration TODO
key_path = "./google_key.json"
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


def step_1_query_and_export():
    credentials = connect.initialize_google_account(key_path)
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
