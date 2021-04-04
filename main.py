from googledataload import bigquery_extract, cloud_connections, lake_transfer, config_loader
import json
import datetime


def run_local_pipeline(sql_query, bigquery_dataset_id, bigquery_table_id, gcs_export_bucket, file_name, gcs_origin_bucket,
                       destination_bucket):
    print("starting pipeline to run google bigquery sql,")
    print("export result table and reliable transfer parquet data file(s) to the azure-based data lake")
    print('step 1 : Query bigquery and store results in table')
    print('step 2 : Export bigquery table as parquet file(s) in google cloud storage')
    print('step 3 : Transfer parquet file(s) from google cloud storage to data lake')
    print('--------------------------------------------------------------------------')

    # Loading Google Cloud credentials
    google_credentials = cloud_connections.initialize_google_account_from_file("./google_key.json")
    project_id = google_credentials.project_id

    # Reading azure storage access key
    storage_key = cloud_connections.load_azure_key_from_file('./azure_key.json')

    # Connect to Azure storage account
    try:
        _ = cloud_connections.initialize_azure_account("googledata", storage_key)
    except Exception as e:
        print(e)

    # Run a query on google bigquery and store the result table
    try:

        destination_table_id = 'testground-97.temp.tpm'
        results = bigquery_extract.query(google_credentials, destination_table_id ,sql_query)
        print('step 1 : queried executed:')
        print(sql_query)
        print(type(results))
    except Exception as e:
        print('step 1 : query error;')
        print(e)

    # Export bigquery table to google cloud storage bucket
    try:

        bigquery_extract.export_as_parquet(google_credentials, bigquery_dataset_id, bigquery_table_id, file_name,
                                           gcs_export_bucket)
        print(
            "step 2 : table exported {}:{}.{} to {} as parquet file".format(project_id, bigquery_dataset_id,
                                                                            bigquery_table_id,
                                                                            gcs_export_bucket)
        )
    except Exception as e:
        print('step 2 : export error:')
        print(e)

    # Transfer files from google cloud storage bucket to the azure storage account container
    try:

        # TODO parametrize
        transfer_start_date = datetime.date(2021, 3, 30)
        transfer_start_time = datetime.time(hour = 20)

        job = lake_transfer.transfer_to_lake(google_credentials, gcs_origin_bucket, destination_bucket,
                                             transfer_start_date, transfer_start_time)
        print('step 3 : returned transferJob: {}'.format(
            json.dumps(job, indent = 4)))
    except Exception as e:
        print('step 3 : transfer error:')
        print(e)


def main():
    try:
        cnf = config_loader.load_config('')
        bigquery_dataset_id = cnf['google_cloud']['bigquery_dataset_id']
        bigquery_table_id = cnf['google_cloud']['bigquery_table_id']
        gcs_export_bucket = cnf['google_cloud']['gcs_export_bucket']
        gcs_origin_bucket = cnf['google_cloud']['gcs_export_bucket']
        file_name = cnf['google_cloud']['file_name']
        azure_destination_bucket = cnf['azure']['destination_bucket']
    except Exception as e:
        print('step 0 : error with config loading:')
        print(e)

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

    run_local_pipeline(sql_query, bigquery_dataset_id, bigquery_table_id, gcs_export_bucket, file_name, gcs_origin_bucket,
                       azure_destination_bucket )


main()
