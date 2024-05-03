from kafka import KafkaProducer
import json


''''
This module will handle all Kafka-related functionality, including sending data to Kafka.


'''

class KafkaClient:
    def __init__(self, servers):
        self.producer = KafkaProducer(bootstrap_servers=servers,
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    def send_data(self, topic, data):
        self.producer.send(topic, value=data)
        self.producer.flush()
