from datetime import datetime, timedelta
import pytz
import asyncio
import json
import asyncio
import json

def market_hours_generator(today_date):

    eastern = pytz.timezone('US/Eastern')
    today_date = datetime.strptime(today_date, '%Y-%m-%d')
    
    
    market_open = datetime(today_date.year, today_date.month, today_date.day, 10, 30, tzinfo=eastern)
    market_close = datetime(today_date.year, today_date.month, today_date.day, 15, 0, tzinfo=eastern)
    
    # Start time: 1 hour before market open
    start_time = market_open - timedelta(hours=1)
    
    # End time: 1 hour after market close
    end_time = market_close + timedelta(hours=1)
    
    # Current time starts at start_time
    current_time = start_time
    
    while current_time <= end_time:
        yield current_time.strftime('%Y-%m-%d %H:%M:%S')
        current_time += timedelta(minutes=5)




async def fetch_stock_data(session,function ,symbol,interval,key,timestamp):
    api_url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey={key}"
    async with session.get(f'{api_url}') as response:
        data = await response.json()
        #print(data)
        try:
            
            return  {"symbol": symbol,'time': f'{timestamp}', "data": data["Time Series (5min)"][f'{timestamp}']}
        except KeyError as e:
            return {"symbol": symbol,'time': f'{timestamp}', "data": f'No data return for this time period:{e}'}




async def fetch_and_produce(session, function ,interval,symbol,key,producer,kafka_topic,timestamp):
    data = await fetch_stock_data(session,function ,symbol,interval,key,timestamp)
    data_json = json.dumps(data)
    kafka_key = symbol.encode('utf-8')
    producer.produce(kafka_topic,key=kafka_key, value= data_json)
    producer.flush() 
    #print(data)

async def fetch_and_produce_batch(session, function,interval,symbols,key,producer,kafka_topic,timestamp):
    tasks = [fetch_and_produce(session, function ,interval,symbol,key,producer,kafka_topic,timestamp) for symbol in symbols]
    await asyncio.gather(*tasks)

def chunk_list(lst, chunk_size):
    """Yield successive chunks from list."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]