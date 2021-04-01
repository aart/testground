from googledataload import cloud_connects

def main():
    print("starting pipeline to run google bigquery sql, export result table and reliable transfer parquet data file(s) to the azure-based data lake")

    #Loading Google Cloud credentials
    key_path="./google_key.json"
    credentials=cloud_connects.initialize_google_account_from_file(key_path)
    project_id=credentials.project_id
    print(project_id)

    #Reading azure storage access key
    storage_key = cloud_connects.load_azure_key_from_file('./azure_key.json')
    print(storage_key)

    try:
        service_client=cloud_connects.initialize_azure_account("googledata", storage_key)
    except Exception as e:
        print(e)



main()
