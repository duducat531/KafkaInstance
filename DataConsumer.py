from kafka import KafkaConsumer
from DBInstance.DBInstance import DBInstance

class DataConsumer:
    def __init__(self, topicName, serverAddr, groupId, dbInstance):
        self.topicName = topicName
        self.serverAddr = serverAddr
        self.groupId = groupId
        self.dbInstance = dbInstance

    def ReceiveAndSaveToDB(self):
        consumer = KafkaConsumer(self.topicName, bootstrap_servers=self.serverAddr)
        for data in consumer:
            # save data to mysql
            self.dbInstance.SaveToDB(data)
