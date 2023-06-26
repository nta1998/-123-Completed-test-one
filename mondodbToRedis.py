import os
from pymongo import MongoClient,errors
from configparser import ConfigParser
from datetime import datetime
import redis
import time

from kafkaProducer import Event

# Config Data
config = ConfigParser()
config.read("config.ini")

# Redis configuration
redis_host = config["RedisInfo"]["redis_host"]
redis_port = config["RedisInfo"]["redis_port"]
redis_db = config["RedisInfo"]["redis_db"]
res_ids = config["RedisInfo"]["res_ids"]

mongodb_uri = config["MongodbInfo"]["mongodb_uri"]
mongodb_db = config["MongodbInfo"]["mongodb_database"]
mongodb_collection = config["MongodbInfo"]["mongodb_collection"]

# Create MongoDB client and connect to the database
client = MongoClient(mongodb_uri)
db = client[mongodb_db]
collection = db[mongodb_collection]

sleep_time = int(config["Time"]["error"])
to_ms = int(config["Date"]["to_ms"])
# Connect to Redis
redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

def poll_data_to_redis():
    os.system('clear')

    while True:

        try:
        # Retrieve data from MongoDB
            with open("lastData.txt", "r") as file:
                first_line = file.readline()
                if first_line != "":
                    last_timestamp = datetime.strptime(first_line.strip(), "%Y-%m-%d %H:%M:%S")
                    data = collection.find({"timestamp": {"$gt":last_timestamp}})     
                else:
                    data = collection.find()
        except FileNotFoundError:
            data = collection.find()
        # Store data in Redis
        try:
            for item in data:
                id,reporter_id ,timestamp, metric_id, metric_value, message = item.values()
                key = f"{reporter_id}:{str(timestamp)}"
                info = f"{item}"
                print(info)
                try: 
                    redis_client.set(key,info)
                except: 
                    print("\n\033[31m" +"The redis server is down Please check the status of the server ,will try to access again in 30 seconds"+"\033[0m\n")
                    time.sleep(sleep_time)
                    poll_data_to_redis()
                with open("lastData.txt", "w") as file:
                    file.write(str(timestamp))
            print("\n\033[32m"+'Data migrated from MongoDB to Redis.'+"\033[0m\n")
        except errors.ServerSelectionTimeoutError:
            print("\n\033[31m" +"The mongodb server is down Please check the status of the server ,will try to access again in 30 seconds"+"\033[0m\n")
            poll_data_to_redis()
        # Wait for 30 sec
        time.sleep(sleep_time)


if __name__ == "__main__":
    
    while True:
        script_manager = input('(1) - start poll data to Redis\n(control + C) - stop\n')

        if script_manager == "1":
            poll_data_to_redis()
        else:
            print("\n\033[31m" + "A wrong key was pressed Try again\n"+"\033[0m")