import os
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential


def get_stock_list():
        
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



    return first_column
 

