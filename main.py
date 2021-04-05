from googledataload import bigquery_extract, cloud_connections, lake_transfer, config_loader
import datetime
import logging
import json

def run_local_pipeline(sql_query, bigquery_dataset_id, bigquery_table_id, gcs_export_bucket, file_name, gcs_origin_bucket,
                       destination_bucket):
    logging.info("starting pipeline to run google bigquery sql")
    logging.info("export result table and reliable transfer parquet data file(s) to the azure-based data lake")
    logging.info('step 1 : Query bigquery and store results in table')
    logging.info('step 2 : Export bigquery table as parquet file(s) in google cloud storage')
    logging.info('step 3 : Transfer parquet file(s) from google cloud storage to data lake')
    logging.info('--------------------------------------------------------------------------')

    # Loading Google Cloud credentials
    google_credentials = cloud_connections.initialize_google_account_from_file("./google_key.json")
    project_id = google_credentials.project_id

    # Reading azure storage access key
    storage_key = cloud_connections.load_azure_key_from_file('./azure_key.json')

    # Connect to Azure storage account
    try:
        _ = cloud_connections.initialize_azure_account("googledata", storage_key)
    except Exception as e:
        logging.error(e)

    # Run a query on google bigquery and store the result table
    try:

        results = bigquery_extract.query(google_credentials, bigquery_dataset_id, bigquery_table_id, sql_query)
        logging.info('step 1 : queried executed:')
        logging.info(sql_query)
    except Exception as e:
        logging.error('step 1 : query error;')
        logging.error(e)

    # Export bigquery table to google cloud storage bucket
    try:

        bigquery_extract.export_as_parquet(google_credentials, bigquery_dataset_id, bigquery_table_id, file_name,
                                           gcs_export_bucket)
        logging.info(
            "step 2 : table exported {}:{}.{} to {} as parquet file".format(project_id, bigquery_dataset_id,
                                                                            bigquery_table_id,
                                                                            gcs_export_bucket)
        )
    except Exception as e:
        logging.error('step 2 : export error:')
        logging.error(e)

    # Transfer files from google cloud storage bucket to the azure storage account container
    try:

        # TODO parametrize
        transfer_start_date = datetime.date.today()
        transfer_start_time = datetime.time(hour = 15)

        job = lake_transfer.transfer_to_lake(google_credentials, gcs_origin_bucket, destination_bucket,
                                             transfer_start_date, transfer_start_time)
        logging.info('step 3 : returned transferJob: {}'.format(
            json.dumps(job, indent = 4)))
    except Exception as e:
        logging.error('step 3 : transfer error:')
        logging.error(e)


def main():
    logging.basicConfig(filename = 'myapp.log', level = logging.INFO)

    try:
        cnf = config_loader.load_config('./config.yaml')
        sql_query = cnf['google_cloud']['sql_query']
        bigquery_dataset_id = cnf['google_cloud']['bigquery_dataset_id']
        bigquery_table_id = cnf['google_cloud']['bigquery_table_id']
        gcs_export_bucket = cnf['google_cloud']['gcs_export_bucket']
        gcs_origin_bucket = cnf['google_cloud']['gcs_export_bucket']
        file_name_prefix = cnf['google_cloud']['file_name_prefix']
        azure_destination_bucket = cnf['azure']['destination_bucket']
    except Exception as e:
        logging.error('step 0 : error with config loading:')
        logging.error(e)


    run_local_pipeline(sql_query, bigquery_dataset_id, bigquery_table_id, gcs_export_bucket, file_name_prefix, gcs_origin_bucket,
                       azure_destination_bucket )


main()
