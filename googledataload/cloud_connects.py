from google.oauth2 import service_account
import unittest
import azure.storage.filedatalake


class TestConnect(unittest.TestCase):
    def test_initialize_google_account(self):
        key_path="../google_key.json"
        credentials=initialize_google_account_from_file(key_path)
        project_id=credentials.project_id
        self.assertEqual(project_id, "testground-97", "issue")

    def test_load_azure_key(self):
        load_azure_key_from_file('../azure_key.json')

    def test_create_azure_directory(azure_key: str):
        initialize_azure_account("googledata", azure_key)
        try:
            global file_system_client
            file_system_client=service_client.create_file_system(file_system="my-file-system")
            try:
                file_system_client.create_directory("my-directory")
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)

# initialize
def load_azure_key_from_file(key_path: str) -> str:
    import json
    with open(key_path) as json_file:
        data=json.load(json_file)
        return data['sorage_key']


# initialize
def initialize_google_account_from_file(key_path: str) -> service_account.Credentials:
    credentials=service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credentials


def initialize_azure_account(storage_account_name: str, storage_account_key: str):
    try:
        global service_client

        service_client=azure.storage.filedatalake.DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format(
                "https", storage_account_name), credential=storage_account_key)
    except Exception as e:
        print("azure error handling")  # TODO
        return e


if '__name__'=='__main__':
    unittest.main()
