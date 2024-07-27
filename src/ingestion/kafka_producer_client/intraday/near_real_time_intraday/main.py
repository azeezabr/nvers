import asyncio
import aiohttp
import time
import json
from kafka import KafkaProducer
import os
from confluent_kafka import Producer
from config import config
from src.common import get_stock_list
from src.common.utils import other_utils
from src.ingestion.utils import utils



key = os.environ.get("KEY")

symbols = get_stock_list.get_stock_list()

rate_limit = 75 
output_json = "stock_data.json"
interval = '5min'
function='TIME_SERIES_INTRADAY'
kafka_topic = "stock-time-series"
#month = '2024-05'
#compact = 'full'
pipe_line_type = 'near-real-time'
#pipe_line_type = 'daily'

#config = config.read_config()

config  = conf = {
    'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'NKLSWDJWCWIE5FQE',
    'sasl.password': '72pEJcSJaSMm2/zbqdhMa23N5dnVD7l2weDQNKC9JWFBIYyBuWadHX0XS5N6PImz'
}
  
producer = Producer(config)


async def main():
    
    timestamps = other_utils.market_hours_generator('2024-05-22')
    async with aiohttp.ClientSession() as session:
        for timestamp in timestamps:
            increament = 0
            for chunk in utils.chunk_list(symbols, rate_limit):
                await utils.fetch_and_produce_batch(session, chunk,timestamp,key,producer,kafka_topic,function,interval,pipe_line_type)
                increament +=1
                print(f'Executed batch {increament} for time {timestamp}')
                await asyncio.sleep(60)  
            print(f'Execution completed for time {timestamp}')
            
        print(f'Done executing for date')


    


if __name__ == "__main__":
    asyncio.run(main())
