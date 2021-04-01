from googledataload import cloud_connections

def main():
    print("starting pipeline to run google bigquery sql, export result table and reliable transfer parquet data file(s) to the azure-based data lake")

    # Loading Google Cloud credentials
    credentials=cloud_connections.initialize_google_account_from_file("./google_key.json")
    project_id=credentials.project_id
    print(project_id)

    # Reading azure storage access key
    storage_key = cloud_connections.load_azure_key_from_file('./azure_key.json')
    print(storage_key)

    # Connect to Azure storage account
    try:
        service_client=cloud_connections.initialize_azure_account("googledata", storage_key)
    except Exception as e:
        print(e)

    # run query
    # export file
    # transfer file

    # TODO monitoring
    # TODO logging

main()
