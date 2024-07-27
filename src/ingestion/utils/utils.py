import asyncio
import aiohttp
import time
import json
from kafka import KafkaProducer
import os
from confluent_kafka import Producer
import logging




def read_stream(self, spark, bootstrap_servers, topic,kafka_username,kafka_password):
        spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
        .option("startingOffsets", "earliest") \
        .load()



# Configure logging to write to a file
log_file_path = '/Users/azeez/Projects/nvers/src/ingestion/ingest_kafka_raw/monthly_intraday/error_log.log' #os.path.join(os.path.dirname(__file__), 'error_log.log')
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

async def fetch_stock_data(session, function, symbol, key, timestamp, interval, pipe_line_type, month='02'):
    api_url_nrt = f"https://www.alphavantage.co/query?function={function}&outputsize=full&symbol={symbol}&interval={interval}&apikey={key}"
    api_url_daily = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey={key}&outputsize=full&month={month}'
    api_url_batch = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key}'
    
    api_url = (api_url_nrt if pipe_line_type == 'near-real-time' else
               api_url_daily if pipe_line_type == 'daily' else
               api_url_batch)

    try:
        async with session.get(api_url) as response:
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/json' in content_type:
                data = await response.json()
            else:
                text = await response.text()
                logging.error(f"Unexpected content type: {content_type}. Response text: {text}")
                raise aiohttp.client_exceptions.ContentTypeError(
                    response.request_info,
                    response.history,
                    message=f"Unexpected content type: {content_type}",
                    headers=response.headers
                )
            
            if pipe_line_type == 'near-real-time':
                return {
                    "symbol": symbol,
                    'time': f'{timestamp}',
                    "data": {
                        "Meta Data": data.get("Meta Data", {}),
                        f"Time Series: {timestamp}": data.get("Time Series (5min)", {}).get(f'{timestamp}', {})
                    }
                }
            elif pipe_line_type == 'daily':
                return {
                    "symbol": symbol,
                    'time': data["Meta Data"]["3. Last Refreshed"],
                    "data": data
                }
            elif pipe_line_type == 'batch':
                return data
            else:
                return 'invalid pipeline type'

   
    except aiohttp.client_exceptions.ContentTypeError as e:
        error_message = f"ContentTypeError: {e} - Returning: Invalid content type: info: symbol - {symbol} - month - {month}"
        logging.error(error_message)
        return {"symbol": symbol, 'time': f'{timestamp}', "data": 'No data returned'}

    except aiohttp.ClientError as e:
        error_message = f"ClientError: {e} - Returning: Client error occurred: info: symbol - {symbol} - month - {month}"
        logging.error(error_message)
        return {"symbol": symbol, 'time': f'{timestamp}', "data": 'No data returned'}

    except Exception as e:
        error_message = f"Unexpected error: {e} - Returning: Unexpected error occurred: info: symbol - {symbol} - month - {month}"
        logging.error(error_message)
        return {"symbol": symbol, 'time': f'{timestamp}', "data": 'No data returned'}





'''
async def fetch_stock_data(session,function ,symbol,key,timestamp,interval,pipe_line_type,month):

    api_url_nrt = f"https://www.alphavantage.co/query?function={function}&outputsize=full&symbol={symbol}&interval={interval}&apikey={key}"
    api_url_daily = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey={key}&outputsize=compact&month={month}'
    api_url_batch = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key}'
    #api_url = api_url_nrt if pipe_line_type == 'near-real-time' else api_url_daily

    api_url = (api_url_nrt if pipe_line_type == 'near-real-time' else
           api_url_daily if pipe_line_type == 'daily' else
           api_url_batch)

    async with session.get(f'{api_url}') as response:
        data = await response.json()
        
        try:
            if pipe_line_type == 'near-real-time':
                #return  {"symbol": symbol,'time': f'{timestamp}', "data":{"Meta Data":data["Meta Data"],"Time Series": {timestamp: data["Time Series (5min)"][f'{timestamp}']}}}
                return  {"symbol": symbol,'time': f'{timestamp}', "data":{"Meta Data":data["Meta Data"],f"Time Series: {timestamp}": data["Time Series (5min)"][f'{timestamp}']}}
            elif pipe_line_type == 'daily': 

                return  {"symbol": symbol,'time': data["Meta Data"]["3. Last Refreshed"], "data":data}
            elif pipe_line_type == 'batch': 
                return data
                
            else:
                return 'invalid pipeline type'

        except KeyError as e:
            return {"symbol": symbol,'time': f'{timestamp}', "data": 'No data returned'}
'''            
            



async def fetch_and_produce(session, function ,symbol,key,producer,kafka_topic,timestamp,interval,pipe_line_type):
    data = await fetch_stock_data(session, function ,symbol,key,timestamp,interval,pipe_line_type)
    data_json = json.dumps(data)
    kafka_key = symbol.encode('utf-8')
    producer.produce(kafka_topic,key=kafka_key, value= data_json)
    producer.flush() 
    #print(data_json)

async def fetch_and_produce_batch(session, symbols,timestamp,key,producer,kafka_topic,function,interval,pipe_line_type):
    tasks = [fetch_and_produce(session, function ,symbol,key,producer,kafka_topic,timestamp,interval,pipe_line_type) for symbol in symbols]
    await asyncio.gather(*tasks)

def chunk_list(lst, chunk_size):
    """Yield successive chunks from list."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]




async def fetch_and_produce_2(session, function, symbol, key, timestamp, interval, pipe_line_type, month):
    data = await fetch_stock_data(session, function, symbol, key, timestamp, interval, pipe_line_type, month)
    # Perform your data processing and pipeline tasks here
    return data

# Function to fetch and produce data for a batch of symbols
async def fetch_and_produce_batch_2(session, symbols, key, function, timestamp, interval, pipe_line_type, month):
    tasks = [fetch_and_produce_2(session, function, symbol, key, timestamp, interval, pipe_line_type, month) for symbol in symbols]
    return await asyncio.gather(*tasks)


def validate_data(record):
    if isinstance(record.get("data"), dict) and "Meta Data" in record["data"] and "Time Series (5min)" in record["data"]:
        return record
    else:
        record["data"] = {
            "Meta Data": {
                "1. Information": "",
                "2. Symbol": "",
                "3. Last Refreshed": "",
                "4. Interval": "",
                "5. Output Size": "",
                "6. Time Zone": ""
            },
            "Time Series (5min)": {}
        }
        return record