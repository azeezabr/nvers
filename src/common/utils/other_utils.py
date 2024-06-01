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




