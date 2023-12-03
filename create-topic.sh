#!/bin/bash

set -e

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
c=0
until [ $c -ge 20 ]; do
    kafka-topics.sh --list --bootstrap-server localhost:9092 && break
    c=$((c+1))
    sleep 5
done

# Create the Kafka topic
echo "Creating Kafka topic..."
kafka-topics.sh --create --topic SWatTopic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

echo "Kafka topic created successfully."
