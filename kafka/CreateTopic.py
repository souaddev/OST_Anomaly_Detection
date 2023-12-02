from confluent_kafka.admin import AdminClient, NewTopic
 
# Kafka broker settings
broker = 'broker:29092'
 
# Create AdminClient instance
admin_client = AdminClient({'bootstrap.servers': broker})
 
# Define the topic configuration (optional)
topic_config = {
    'cleanup.policy': 'delete',  # Example configuration, adjust as needed
    'retention.ms': '3600000'
}
 
# Define the topic
topic_name = 'SWatTopic'
 
# Create a NewTopic instance
new_topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1, config=topic_config)
 
# Create the topic
admin_client.create_topics([new_topic])
 
 
topics = admin_client.list_topics().topics
 
print("Available topics:")
for topic in topics:
    print(topic)