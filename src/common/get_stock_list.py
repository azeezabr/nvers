import os
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential
import re



def extract_stock_symbols(log_file_path):
        symbols = set()  # Use a set to store distinct symbols
        symbol_pattern = re.compile(r'symbol - (\w+)')
        
        with open(log_file_path, 'r') as file:
            for line in file:
                match = symbol_pattern.search(line)
                if match:
                    symbols.add(match.group(1))
        
        return list(symbols)  # Convert the set to a list

log_file_path = '/Users/azeez/Projects/nvers/src/ingestion/ingest_kafka_raw/monthly_intraday/error_log.log'
stock_symbols = extract_stock_symbols(log_file_path)


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
            if columns[0].find('^') == -1 and columns[0] not in stock_symbols:
                first_column.append(columns[0])



    return first_column

#lst = get_stock_list()
#print(len(lst))

