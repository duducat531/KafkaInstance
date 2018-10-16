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

if __name__ == '__main__':
    logDataConsumer = DataProducer(topicName='ebay_log', serverAddr='localhost:9092', groupId='test_log_data_producer')
    logData = {'corr_id': 'ejsQ5ufDFCmyHG6v',
               'url_api': 'sample/login',
               'node_id': 'NSflDxKmr2AR',
               'ri': 'OgXI76Q5z9++',
               'parent_ri': 'NSflDxKmr2AAR'}
    logDataConsumer.PublishData(logData)