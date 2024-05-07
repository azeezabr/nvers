import aiohttp
import asyncio

# List of stock symbols
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']

# Base URL of the API
base_url = "https://www.alphavantage.co/query"

# Your API key
api_key = "demo"

async def fetch_data(session, symbol):
    params = {
        "function": "OVERVIEW",
        "symbol": symbol,
        "apikey": api_key
    }
    
    async with session.get(base_url, params=params) as response:
        data = await response.json()
        print(f"Data for {symbol}:")
        print(data)
        print()  # Just for spacing

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, symbol) for symbol in symbols]
        await asyncio.gather(*tasks)

# Run the asyncio main function
if __name__ == "__main__":
    asyncio.run(main())
