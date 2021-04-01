from googledataload import cloud_connections, bigquery_extract


def run_pipeline():
    print("starting pipeline to run google bigquery sql, export result table and reliable transfer parquet data file(s) to the azure-based data lake")

    # Loading Google Cloud credentials
    google_credentials=cloud_connections.initialize_google_account_from_file("./google_key.json")
    project_id= google_credentials.project_id
    print(project_id)

    # Reading azure storage access key
    storage_key = cloud_connections.load_azure_key_from_file('./azure_key.json')
    print(storage_key)

    # Connect to Azure storage account
    try:
        service_client=cloud_connections.initialize_azure_account("googledata", storage_key)
    except Exception as e:
        print(e)

    # Run a query on google bigquery and store the result table
    try:
        sql_query="""
            SELECT
              CONCAT(
                'https://stackoverflow.com/questions/',
                CAST(id as STRING)) as url,
              view_count
            FROM `bigquery-public-data.stackoverflow.posts_questions`
            WHERE tags like '%google-bigquery%'
            ORDER BY view_count DESC
            LIMIT 10"""
        key_path="./google_key.json"
        # TODO assert
        bigquery_extract.query( google_credentials, sql_query)
        print('queried')
    except Exception as e:
        print('query error')
        print(e)

    # Export bigquery table to google cloud storage bucket
    try:
        bigquery_dataset_id="test"
        bigquery_table_id="housing"
        gcs_export_bucket="testground-97"

        bigquery_extract.export( google_credentials, bigquery_dataset_id, bigquery_table_id, gcs_export_bucket)
        print(
            "Exported {}:{}.{} to {}".format(project_id, bigquery_dataset_id, bigquery_table_id, gcs_export_bucket)
        )
    except Exception as e:
        print('query error')
        print(e)

    # TODO
    # Transfer files from google cloud storage bucket to the azure storage account container
    lake_destination_bucket="azure-proxy"


    # TODO monitoring
    # TODO logging

def main():
    run_pipeline()

main()
