import requests


'''
This module will handle fetching data from the stock API.
'''

class StockAPIClient:
    def __init__(self, api_url):
        self.api_url = api_url

    def fetch_stock_data(self):
        response = requests.get(self.api_url)
        if response.status_code == 200:
            return response.json()
        else:
            return None
