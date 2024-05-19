import os
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential



account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')


def get_service_client_account_key(account_name, account_key) -> DataLakeServiceClient:

    account_url = f"https://{account_name}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url, credential=account_key)

    return service_client

file_system_name = 'bronze'
service_cl = get_service_client_account_key(account_name,account_key)

file_system_client = service_cl.get_file_system_client(file_system=file_system_name)
directory_name = '2024-05-09'

'''def list_directory_contents(file_system_client: FileSystemClient, directory_name: str):
    paths = file_system_client.get_paths(path=directory_name)

    for path in paths:
        print(path.name + '\n')



list_directory_contents(file_system_client,directory_name)
'''
file_path ='2024-05-10/TickerList'

file_client = file_system_client.get_file_client(file_path)

download = file_client.download_file()
downloaded_bytes = download.readall()
content = downloaded_bytes.decode('utf-8')

first_column = []
for line in content.split('\n')[1:]:
    if line:
        columns = line.split(',')
        if columns[6].strip() == 'United States':
            first_column.append(columns[0])


print(len(first_column))
 

'''
def list_directory_contents(self, file_system_client: FileSystemClient, directory_name: str):
    paths = file_system_client.get_paths(path=directory_name)

    for path in paths:
        print(path.name + '\n')

print(list_directory_contents())

'''

'''
file_system_name = 'your_file_system_name'
file_path = 'your_file_path'

#file_system_client.get_file_client(file_path)

file_service = get_service_client_account_key(account_name, account_key) 
ticker_file = file_service.service_client.file_client(file_system_name,file_path)


def read_tickers(file_client):
    try:
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        tickers = downloaded_bytes.decode('utf-8').split()
        return tickers
    except Exception as e:
        print(e)
        raise
'''

