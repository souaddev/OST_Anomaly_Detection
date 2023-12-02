from confluent_kafka import Consumer, KafkaError

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092', 
    'group.id': '1',
    'auto.offset.reset': 'earliest'
}

# Kafka topic
topic = 'SWatTopic'

# Create Kafka consumer
consumer = Consumer(kafka_config)
consumer.subscribe([topic])

# Poll for messages
while True:
    msg = consumer.poll(1.0)  # Adjust the timeout as needed
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event, not an error
            continue
        else:
            print(msg.error())
            break
    print('Received message: {}'.format(msg.value().decode('utf-8')))



