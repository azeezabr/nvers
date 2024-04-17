from confluent_kafka import Producer
import socket

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def main():
    # Configuration for the Kafka Producer
    conf = {
        'bootstrap.servers': "nvs-event-hub-namespace.servicebus.windows.net:9093",
        'client.id': socket.gethostname(),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': 'Endpoint=sb://nvs-event-hub-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=lyG937GhPMtGPyJ5+UH/w9fGR88xZ/FHM+AEhBtJRvA=',
        'ssl.ca.location': '/opt/homebrew/etc/openssl@3/cert.pem',  # Adjust this path if necessary
        'request.timeout.ms': 60000,
        'session.timeout.ms': 30000
    }

    # Create Producer
    producer = Producer(**conf)

    # Topic name, which is your Event Hub name
    topic = 'stock-hub'

    # Send messages
    try:
        for i in range(1000):
            message = f'Hello World {i}'
            producer.produce(topic, message.encode('utf-8'), callback=acked)
            producer.poll(1)
    except Exception as e:
        print(f'An error occurred: {e}')
    finally:
        # Wait for any outstanding messages to be delivered and report delivery result
        producer.flush(30)  # Wait up to 30 seconds.

if __name__ == '__main__':
    main()
