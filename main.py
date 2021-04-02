from googledataload import bigquery_extract, cloud_connections, lake_transfer,config
import json
import datetime

# TODO integrate secrets in an external system: key vault, ....
# TODO integrate environments aspects UAT, PRD, ...
# TODO add monitoring
# TODO add logging
# TODO add code comments

def run_pipeline_example():
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
        results = bigquery_extract.query(google_credentials, sql_query)
        print('step 1 : queried executed:')
        print(sql_query)
        print(type(results))
    except Exception as e:
        print('step 1 : query error;')
        print(e)

    # Export bigquery table to google cloud storage bucket
    try:
        bigquery_dataset_id = "test"
        bigquery_table_id = "housing"
        gcs_export_bucket = "testground-97"
        file_name = "housing.parquet"

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
        destination_bucket = "azure-proxy"
        gcs_origin_bucket = "testground-97"
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
    #run_pipeline_example()
    cnf = config.load_config()

    bigquery_dataset_id =  cnf['google_cloud']
    print(bigquery_dataset_id)
    # bigquery_table_id =  cnf['google_cloud']['bigquery_dataset_id']
    # gcs_export_bucket =  cnf['google_cloud']['bigquery_dataset_id']
    # file_name =  cnf['google_cloud']['bigquery_dataset_id']
    # destination_bucket =  cnf['azure']['azure']

main()
