import json
from kafka import KafkaConsumer
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
 
# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'swat_test2'
consumerGID = 'swat_test_gid-1'
 
# InfluxDB configuration (Dear teammate, please, replace this token with urs)
token = "n2JFLRm9VaWepZ4zwk8YJlhR5AVEa_WIk2KCn2xhM9cMYW5s8qeMH5r0yX9wmOdyEs_Nq5Evnpwpm97sPdfP5A=="
#token = "n2JFLRm9VaWepZ4zwk8YJlhR5AVEa_WIk2KCn2xhM9cMYW5s8qeMH5r0yX9wmOdyEs_Nq5Evnpwpm97sPdfP5A=="
org = "dataCrew"
bucket = "anomaly-detection"
 
 
def convert_attack_to_double(attack_value):
    if attack_value.lower() == "normal":
        return 0.0
    elif attack_value.lower() == "attack":
        return 1.0
    else:
        return float(attack_value)



def process_kafka_message(message):
    data = json.loads(message.value)
 
    # Convert "Normal/Attack" to double
    data["Normal/Attack"] = convert_attack_to_double(data["Normal/Attack"])
 
    # Remove leading spaces from the timestamp string
    timestamp_str = data["Timestamp"].strip()
 
    # Convert timestamp to datetime
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
 
    # Convert datetime to Unix timestamp in seconds
    timestamp_seconds = int(timestamp.timestamp())
 
    # InfluxDB point
    point = Point("Swat-2028").field('Timestamp', timestamp_seconds)
    for key, value in data.items():
        if key != "Timestamp":
            point.field(key, float(value))
 
    return point

 
 
def main():
    try:
        # Kafka consumer
        consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id=consumerGID)
 
        # InfluxDB client
        with InfluxDBClient(url="http://localhost:8086", token=token, org=org, debug=False) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
 
            for message in consumer:
                try:
                    # Process Kafka message
                    influx_point = process_kafka_message(message)
 
                    # Write to InfluxDB
                    write_api.write(bucket=bucket, record=influx_point)
 
                    print(f"Data written to InfluxDB: {influx_point.to_line_protocol()}")
                except Exception as e:
                    print(f"Error processing message: {e}")
 
    except Exception as e:
        print(f"Error: {e}")
 
if __name__ == "__main__":
    main()