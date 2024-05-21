import asyncio
import aiohttp
import time
import json
from kafka import KafkaProducer
import os
from confluent_kafka import Producer
from src.common import (
    utils, 
    config,
    get_stock_list
    )



key = os.environ.get("KEY")

symbols = get_stock_list.get_stock_list()

rate_limit = 75 
output_json = "stock_data.json"
interval = '5min'
function='TIME_SERIES_INTRADAY'
kafka_topic = "stock-time-series"
#month = '2024-05'
#compact = 'full'

config = config.read_config()
  
producer = Producer(config)


async def main():
    
    timestamps = utils.market_hours_generator('2024-05-17')
    async with aiohttp.ClientSession() as session:
        for timestamp in timestamps:
            increament = 0
            for chunk in utils.chunk_list(symbols, rate_limit):
                await utils.fetch_and_produce_batch(session, function,interval,symbols,key,producer,kafka_topic,timestamp)
                increament +=1
                print(f'Executed batch {increament} for time {timestamp}')
                await asyncio.sleep(60)  
            print(f'Execution completed for time {timestamp}')
            
        print(f'Done executing for date')


if __name__ == "__main__":
    asyncio.run(main())
