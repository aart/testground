import yaml

def load_config(config_path: str):

    #TODO externalize into a yaml config file
    yaml_config = """
google_cloud:
    sql_query: "SELECT * FROM `testground-97.test.housing`"
    bigquery_dataset_id: "transit"
    bigquery_table_id: "housing"
    gcs_export_bucket: "testground-97"
    file_name_prefix: "housing.parquet"
azure:
    destination_bucket: "azure-proxy"
"""

    config = yaml.load(yaml_config, Loader = yaml.Loader)
    return config
