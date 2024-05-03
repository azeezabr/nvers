import json
import time
from faker import Faker
import argparse
from confluent_kafka import Producer
import socket

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def build_kafka_config(namespace, key_name, key_value):
    """
    Construct the Kafka configuration using the given components.
    """
    return {
        'bootstrap.servers': f"{namespace}.servicebus.windows.net:9093",
        'client.id': socket.gethostname(),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': key_name,
        'sasl.password': key_value,
        'ssl.ca.location': '/opt/homebrew/etc/openssl@3/cert.pem',  # Adjust this path if necessary
    }

def main(params):
    config = build_kafka_config(params.namespace, params.key_name, params.key_value)
    fake = Faker()
    producer = Producer(**config)

    try:
        while True:
            data = {'OrderID': fake.uuid4(),
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
            data_json = json.dumps(data)
            producer.produce(params.event_hub_name, data_json.encode('utf-8'), callback=acked)
            producer.poll(0)
            time.sleep(1)
    finally:
        producer.flush(30)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate ecommerce transactions')
    parser.add_argument('--namespace', required=True, help='Namespace of the Kafka server')
    parser.add_argument('--event_hub_name', required=True, help='Name of the Kafka Topic')
    parser.add_argument('--key_name', required=True, help='SASL Username')
    parser.add_argument('--key_value', required=True, help='SASL Password')
    
    args = parser.parse_args()
    main(args)
