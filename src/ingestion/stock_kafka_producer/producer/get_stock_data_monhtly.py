import asyncio
import aiohttp
import time
import json
from kafka import KafkaProducer
import os
from confluent_kafka import Producer
from src.common import utils, config, get_stock_list



key = os.environ.get("KEY")

symbols = get_stock_list.get_stock_list()

rate_limit = 75 
output_json = "stock_data.json"
interval = '5min'
function='TIME_SERIES_INTRADAY'
kafka_topic = "stock-time-series"
month = '2024-05'
compact = 'full'

config = config.read_config()
  
producer = Producer(config)

async def fetch_stock_data(session,function ,symbol,key):
    api_url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey={key}&month={month}&outputsize={compact}"
    async with session.get(f'{api_url}') as response:
        data = await response.json()
        print(data)
        try:
            
            return  {"symbol": symbol,'time': f'{month}', "data": data["Time Series (5min)"]}
        except KeyError as e:
            return {"symbol": symbol,'time': f'{month}', "data": f'No data return for this time period:{e}'}




async def fetch_and_produce(session, function ,symbol,key,producer,kafka_topic):
    data = await fetch_stock_data(session, function ,symbol,key)
    data_json = json.dumps(data)
    kafka_key = symbol.encode('utf-8')
    producer.produce(kafka_topic,key=kafka_key, value= data_json)
    producer.flush() 
    #print(data)

async def fetch_and_produce_batch(session, symbols):
    tasks = [fetch_and_produce(session, function ,symbol,key,producer,kafka_topic) for symbol in symbols]
    await asyncio.gather(*tasks)

def chunk_list(lst, chunk_size):
    """Yield successive chunks from list."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

async def main():
    
    async with aiohttp.ClientSession() as session:
            increament = 0
            for chunk in chunk_list(symbols, rate_limit):
                await fetch_and_produce_batch(session, chunk)
                increament +=1
                print(f'Executed batch {increament} for time {month}')
                await asyncio.sleep(60)  
            print(f'Execution completed for time {month}')
            
    print(f'Done executing for {month}')

# Run the async main function
asyncio.run(main())

   
