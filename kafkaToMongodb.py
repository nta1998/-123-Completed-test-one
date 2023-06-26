import os
from kafka import KafkaConsumer
from kafka import errors as Kerrors
from pymongo import MongoClient,errors
from configparser import ConfigParser
from kafkaProducer import Event
from datetime import datetime
import time
import json

config = ConfigParser()
config.read("config.ini")

kafka_server = config["KafkaInfo"]["kafka_server"]
kafka_topic = config["KafkaInfo"]["kafka_topic"]
group_id = config["KafkaInfo"]["group_id"]

mongodb_uri = config["MongodbInfo"]["mongodb_uri"]
mongodb_db = config["MongodbInfo"]["mongodb_database"]
mongodb_collection = config["MongodbInfo"]["mongodb_collection"]

sleep_time = int(config["Time"]["error"])

# Create MongoDB client and connect to the database
client = MongoClient(mongodb_uri)
db = client[mongodb_db]
collection = db[mongodb_collection]

# Create Kafka consumer
def Creatr_consumer ():
    consumer = KafkaConsumer(kafka_topic,
        bootstrap_servers= kafka_server,
        group_id = group_id,
        enable_auto_commit = True,
        auto_offset_reset = "earliest",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    return consumer

def poll_data_to_mongodb():
    
    try: 
        consumer = Creatr_consumer()
    except Kerrors.NoBrokersAvailable: 
        print("\n\033[31m" +"The kafka server is down Please check the status of the server ,will try to access again in 30 seconds"+"\033[0m\n")
        time.sleep(sleep_time)
        poll_data_to_mongodb()

    try:
        client.admin.command("ismaster")

    except errors.ServerSelectionTimeoutError:
        print("\n\033[31m" +"The mongodb server is down Please check the status of the server ,will try to access again in 30 seconds"+"\033[0m\n")
        poll_data_to_mongodb() 
        
    for message in consumer:
        
        kafkaData = message.value
        data = Event(kafkaData["reporter_id"],kafkaData["timestamp"],kafkaData["metric_id"],kafkaData["metric_value"],kafkaData["message"])
        data.timestamp = datetime.strptime(data.timestamp, '%Y-%m-%d %H:%M:%S')
        
        try:
            collection.insert_one(data.__dict__)
        except errors.ServerSelectionTimeoutError:
            print("\n\033[31m" +"The mongodb server is down Please check the status of the server ,will try to access again in 30 seconds"+"\033[0m\n")
            poll_data_to_mongodb() 

        print(kafkaData)

os.system('clear')

if __name__ == "__main__":
    
    while True :
        script_manager = input('(1) - start poll data to mongodb\n(control + C) - stop\n')

        if script_manager == "1":
            poll_data_to_mongodb() 
        else:
            print("\n\033[31m" + "A wrong key was pressed Try again\n"+"\033[0m")
        
