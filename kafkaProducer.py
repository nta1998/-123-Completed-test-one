import os
from kafka import KafkaProducer,errors
from configparser import ConfigParser
from datetime import datetime
import random
import json
import time


config = ConfigParser()
config.read("config.ini")

class Event:
    def __init__(self, reporter_id, timestamp, metric_id, metric_value, message):
        self.reporter_id:int = reporter_id
        self.timestamp:datetime = timestamp
        self.metric_id:int = metric_id
        self.metric_value:int = metric_value
        self.message:str = message

# Create Kafka producer
kafka_server = config["KafkaInfo"]["kafka_server"]
kafka_topic = config["KafkaInfo"]["kafka_topic"]

min = int(config["random"]["min"])
max = int(config["random"]["max"])

sleep_time = int(config["Time"]["sleep"])
error_time = int(config["Time"]["error"])

start_id = config["id"]["start_id"]
def new_producer():
    os.system('clear')
    
    start_id = int(config["id"]["id"])
    default_id = int(config["id"]["min_id"])

    try:
        producer = KafkaProducer(bootstrap_servers=kafka_server,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    except errors.NoBrokersAvailable:
                print("\n\033[31m" +"kafka server is down Please check the status of the server ,will try to access again in 30 seconds"+"\033[0m\n")
                time.sleep(error_time)
                new_producer()

    if int(start_id) != 0:
        generator_id = int(start_id)
    else:
        generator_id = default_id

    print("\n\033[32m"+"producer started working to stop that Press control + C"+"\033[0m\n")
    
    while True:
        # Create the message
        reporter_id = generator_id
        timestamp = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        metric_id = random.randint(min, max)
        metric_value = random.randint(min, max)
        message = config["KafkaInfo"]["message"]

        # Create Event object
        event = Event(reporter_id, timestamp, metric_id, metric_value, message)

        # Send message to Kafka
        producer.send(kafka_topic, value=event.__dict__,partition=0)
        producer.flush()

        generator_id += 1

        config.set('id','id',str(generator_id))
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
            
        # Sleep for 1 second
        time.sleep(sleep_time)

if __name__ == '__main__':

    while True:
        script_manager = input('(1) - start\n(2) - reset id\n(control + C) - stop\n')
        if script_manager == "1" :
            new_producer()
        elif script_manager == "2":
            print("\n\033[32m"+"The id reset and returned to 0"+"\033[0m\n")
            config.set('id','id',start_id)
            with open('config.ini', 'w') as configfile:
                config.write(configfile)
            configfile.close()
        else:
            print("\n\033[31m" + "A wrong key was pressed Try again\n"+"\033[0m")