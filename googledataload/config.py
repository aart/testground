import yaml

def load_config():

    yaml_config = """
google_cloud:
    bigquery_dataset_id = "test"
    bigquery_table_id = "housing"
    gcs_export_bucket = "testground-97"
    file_name = "housing.parquet"
azure:
    destination_bucket = "azure-proxy"
"""

    config = yaml.load(yaml_config, Loader = yaml.Loader)
    return config
