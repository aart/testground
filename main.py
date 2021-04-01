from googledataload import cloud_connections
from googledataload import bigquery_extract

def main():
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


    # configuration TODO
    bigquery_dataset_id="test"
    bigquery_table_id="housing"
    gcs_origin_bucket="testground-97"
    lake_destination_bucket="azure-proxy"

    # export file
    # transfer file

    # TODO monitoring
    # TODO logging

main()
