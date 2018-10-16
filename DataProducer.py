# MetricCollector
from kafka import KafkaProducer
import json

class DataProducer:
    def __init__(self, topicName, serverAddr):
        self.topicName = topicName
        self.serverAddr = serverAddr
        self.producer = KafkaProducer(bootstrap_servers=serverAddr, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def PublishData(self, data):
        self.producer.send(self.topicName, data)
