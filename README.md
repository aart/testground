# REQUIREMENTS
    - A Google Cloud service account key-file providing access to:
        - BigQuery
        - Google Cloud Storage
        - Google Cloud Storage Transfer

        A Google Cloud service account key-file is a json file with the following structure
        {
          "type": "service_account",
          "project_id": "*******************",
          "private_key_id": "*******************",
          "private_key": "-----BEGIN PRIVATE KEY-----*******************-----END PRIVATE KEY-----\n",
          "client_email": "*******************",
          "client_id": "*******************",
          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          "token_uri": "https://oauth2.googleapis.com/token",
          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/*******************.iam.gserviceaccount.com"
        }


    - A Azure Cloud storage account access key providing access to:
        - A Storage Account File container

# DESIGN
    - The solution desing is described in the link below:
        - TODO: INCLUDE LINK

# CONFIGURATION
    Configuration is done in a a yaml file with the following structure

        google_cloud:
            bigquery_dataset_id: "*******************"
            bigquery_table_id: "*******************"
            gcs_export_bucket: "*******************"
            file_name: "*******************"
        azure:
            destination_bucket: "*******************"