from confluent_kafka import Producer
import csv
import json
import os
import time

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'python-producer'
}

# Kafka topic
topic = 'SWatTopic'

import os

csv_file_path = './dataset/SWat_merged.csv' 

if os.path.isfile(csv_file_path):
    print(f"The file {csv_file_path} exists.")
else:
    print(f"The file {csv_file_path} does not exist.")


# Create Kafka producer
producer = Producer(kafka_config)

# Read CSV and send to Kafka
with open(csv_file_path, 'r') as file:
    csv_reader = csv.DictReader(file)
    sleep = 0
    for row in csv_reader:
        # Convert each row to JSON and send to Kafka
        message = json.dumps(row).encode('utf-8')
        producer.produce(topic, key='key', value=message)

        sleep += 1

        if(sleep % 20) == 0:
            time.sleep(10)


# Close the producer
producer.flush()
