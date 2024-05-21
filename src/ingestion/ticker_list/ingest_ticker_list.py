import datetime
import logging
import os

import azure.functions as func
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

        ingest_ticker()
    else:
        ingest_ticker()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)



 
logging.info('funcation called')
def ingest_ticker():
    url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&download=true'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }

    logging.info('URL gotten')


    resp = requests.get(url, headers=headers)
    json_data = resp.json()
    df = pd.DataFrame(json_data['data']['rows'], columns=json_data['data']['headers'])

    credential = DefaultAzureCredential()
    
    account_name = os.environ["STORAGE_ACCOUNT_NAME"]
    file_system_name = os.environ["FILE_SYSTEM_NAME"]
    storage_account_key = os.environ["STORAGE_ACCOUNT_KEY"]



    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    directory_name = current_date #f'stocklist {current_date}'

 

    file_name = "TickerList"
    
    service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=storage_account_key)


    file_system_client = service_client.get_file_system_client(file_system_name)


    directory_client = file_system_client.get_directory_client(directory_name)

    file_client = directory_client.create_file(file_name)
    file_contents = df.to_csv(index=False)
    logging.info('ready to upload file')
    
    file_client.upload_data(file_contents, overwrite=True)
    logging.info('file uploaded')

    file_client.flush_data(len(file_contents))

    