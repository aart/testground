from googledataload import cloud_connects

def main():
    print("starting main")
    print(cloud_connects.load_azure_key_from_file('./azure_key.json'))

main()
