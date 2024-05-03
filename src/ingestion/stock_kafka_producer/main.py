from producer.kafka_client import KafkaClient
from producer.stock_api_client import StockAPIClient
import time


'''
This is the main script that uses the modules to fetch data from the stock API
 and send it to Kafka.


'''

def main():
    # Configuration
    kafka_servers = ['localhost:9092']
    kafka_topic = 'stock_topic'
    stock_api_url = 'URL_TO_STOCK_API'  # Replace with your actual URL

    # Initialize clients
    kafka_client = KafkaClient(servers=kafka_servers)
    stock_api_client = StockAPIClient(api_url=stock_api_url)

    # Main loop to fetch and send data
    while True:
        stock_data = stock_api_client.fetch_stock_data()
        if stock_data:
            kafka_client.send_data(kafka_topic, stock_data)
        time.sleep(60)  # Wait for 60 seconds before fetching again

if __name__ == "__main__":
    main()
