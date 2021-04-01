from google.oauth2 import service_account
import unittest
import azure.storage.filedatalake


class TestModuleFunctions(unittest.TestCase):
    def test_initialize_google_account(self):
        key_path="../google_key.json"
        credentials=initialize_google_account_from_file(key_path)
        project_id=credentials.project_id
        self.assertEqual(project_id, "testground-97", "issue")

    def test_load_azure_key(self):
        load_azure_key_from_file('../azure_key.json')

    def test_create_azure_directory( storage_key: str):
        try:
            service_client=initialize_azure_account("googledata",   storage_key)
            try:
                file_system_client=service_client.create_file_system(file_system="my-file-system")
                try:
                    file_system_client.create_directory("my-directory")
                except Exception as e:
                    print(e)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)


# initialize
def load_azure_key_from_file(key_path: str) -> str:
    import json
    with open(key_path) as json_file:
        data=json.load(json_file)
        return data['storage_key']


# initialize
def initialize_google_account_from_file(key_path: str) -> service_account.Credentials:
    credentials=service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credentials


def initialize_azure_account(storage_account_name: str, storage_account_key: str) -> azure.storage.filedatalake.DataLakeServiceClient:
    try:
        service_client=azure.storage.filedatalake.DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format(
                "https", storage_account_name), credential=storage_account_key)

        return service_client
    except Exception as e:
        print("azure error handling")  # TODO
        raise e

if '__name__'=='__main__':
    unittest.main()
