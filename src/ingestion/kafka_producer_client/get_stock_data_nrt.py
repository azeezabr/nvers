#%pip install -r /Workspace/Repos/a.azeez@techrole.ca/nvers/requirements.txt


import asyncio
import aiohttp
import time
import json
from kafka import KafkaProducer
import os
from confluent_kafka import Producer
from src.common.get_stock_list import get_stock_list
from src.common.utils import other_utils
from config.config import read_config



key = os.environ.get("KEY")

symbols = get_stock_list()

rate_limit = 75 
output_json = "stock_data.json"
interval = '5min'
function='TIME_SERIES_INTRADAY'
kafka_topic = "stock-time-series"
#month = '2024-05'
#compact = 'full'

config = read_config()
  
producer = Producer(config)

async def fetch_stock_data(session,function ,symbol,key,timestamp):
    api_url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey={key}"
    async with session.get(f'{api_url}') as response:
        data = await response.json()
        #print(data)
        try:
            
            return  {"symbol": symbol,'time': f'{timestamp}', "data": data["Time Series (5min)"][f'{timestamp}']}
        except KeyError as e:
            return {"symbol": symbol,'time': f'{timestamp}', "data": f'No data return for this time period:{e}'}




async def fetch_and_produce(session, function ,symbol,key,producer,kafka_topic,timestamp):
    data = await fetch_stock_data(session, function ,symbol,key,timestamp)
    #if data['data'] == 'No data':
    #    pass
    #else: 
    data_json = json.dumps(data)
    kafka_key = symbol.encode('utf-8')
    producer.produce(kafka_topic,key=kafka_key, value= data_json)
    producer.flush() 
    #print(data)

async def fetch_and_produce_batch(session, symbols,timestamp):
    tasks = [fetch_and_produce(session, function ,symbol,key,producer,kafka_topic,timestamp) for symbol in symbols]
    await asyncio.gather(*tasks)

def chunk_list(lst, chunk_size):
    """Yield successive chunks from list."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

async def main():
    
    timestamps = other_utils.market_hours_generator('2024-05-21')
    async with aiohttp.ClientSession() as session:
        for timestamp in timestamps:
            increament = 0
            for chunk in chunk_list(symbols, rate_limit):
                await fetch_and_produce_batch(session, chunk,timestamp)
                increament +=1
                print(f'Executed batch {increament} for time {timestamp}')
                await asyncio.sleep(60)  
            print(f'Execution completed for time {timestamp}')
            
        print(f'Done executing for date')

# Run the async main function
asyncio.run(main())

   
