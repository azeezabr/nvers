import json
import time
from faker import Faker
import argparse
from azure.eventhub import EventHubProducerClient, EventData

def main(params):
    # Azure Event Hub parameters
    connection_str = params.connection_str
    event_hub_name = params.event_hub_name
    
    fake = Faker()

    # Create a producer client to send messages to the event hub
    producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=event_hub_name)

    try:
        while True:
            # Generate a random e-commerce data point
            data = {
                'OrderID': fake.uuid4(),
                'OrderItemsID': fake.uuid4(),
                'CustomerID': fake.uuid4(),
                'Status': fake.random_element(['pending', 'shipped', 'delivered', 'Cancelled']),
                'ProductID': fake.uuid4(),
                'Category': fake.random_element(['Electronics', 'Clothing', 'Home & Kitchen', 'Beauty', 'Sports & Outdoors', 'Books']),
                'Sex': fake.random_element(['Male', 'Female']),
                'Age': fake.random_int(min=18, max=70),
                'ProductName': fake.catch_phrase(),
                'Brand': fake.company(),
                'Websites': fake.random_element(['Amazon', 'eBay', 'Walmart', 'Target', 'Best Buy', 'Facebook']),
                'customerName': fake.name(),
                'Inventory': fake.random_element(['In Stock', 'Out of Stock']),
                'Location': fake.city(),
                'Price': fake.pyfloat(min_value=10.0, max_value=1000.0, right_digits=2),
                'TotalPrice': fake.pyfloat(min_value=50.0, max_value=5000.0, right_digits=2),
                'Quantity': fake.random_int(min=1, max=5),
                'OrderDate': fake.date_this_decade().isoformat(),  # Convert date to string using isoformat()
                'ShippingAddress': fake.address(),
                'PaymentMethod': fake.random_element(['Credit Card', 'PayPal', 'Apple Pay', 'Google Pay', 'Debit Card'])
            }

            # Convert the data to JSON
            data_json = json.dumps(data)

            # Create a batch
            batch = producer.create_batch()

            # Add events to the batch.
            batch.add(EventData(data_json))

            # Send the batch of events to the event hub
            producer.send_batch(batch)

            # Wait for a short period before generating the next data point
            time.sleep(1)
    finally:
        # Close the producer
        producer.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate ecommerce transactions')
    parser.add_argument('--connection_str', required=True, help='Azure Event Hub connection string')
    parser.add_argument('--event_hub_name', required=True, help='Azure Event Hub name')

    args = parser.parse_args()
    main(args)
