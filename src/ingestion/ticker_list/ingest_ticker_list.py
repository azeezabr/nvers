import requests
from bs4 import BeautifulSoup
import pandas as pd

#nasdaq_url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&exchange=NASDAQ&download=true"
 
url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&download=true'

headers = {
	'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
	}

resp = requests.get(url,headers=headers)
json_data = resp.json()
df = pd.DataFrame(json_data['data']['rows'],columns=json_data['data']['headers'])
#df.to_csv('nasdaq.csv',index=False)
print(json_data)