import logging
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from heavy_hitters import HeavyHittersSketch
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB Configuration
influxdb_url = 'http://localhost:8086'
token = '__X7OLddFCsUSr6rQh1zl5AZx45FGw_bUDUWGg8VvYNQB2uyaswDL7w8B5czMfOLCM8r2YZqEkpZ4thWaJ0maA=='
org = 'OST'
bucket = '44785268377f0784'

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("HeavyHittersSketchAIT202") \
            .getOrCreate()
        s_conn.conf.set("spark.executor.memory", "2g")
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
    return s_conn


def connect_to_kafka(spark):
    try:
        spark_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "swat_data") \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 50) \
            .load()
        logging.info("Connected to Kafka successfully")
        return spark_df
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
        return spark_df

def write_to_influxdb(data, client):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    for row in data.collect():
        point = Point("HeavyHitters").tag("item", row['item']).field("frequency", row['frequency']).time(row['Timestamp'], WritePrecision.NS)
        write_api.write(bucket, org, point)

def save_to_csv(data, epoch_id):
    csv_file_path = f'OST_Anomaly_Detection-main\spark\sketches\Heavy_hitters_CSV/heavy_hitters_epoch_{epoch_id}.csv'  # Update the path
    data.toPandas().to_csv(csv_file_path, index=False)
    print(f"Heavy hitters data saved to {csv_file_path}")

def process_batch(batch_df, epoch_id, heavy_hitters_sketch):
    column_to_track = "AIT202"
    heavy_hitters = heavy_hitters_sketch.process_stream(batch_df.select(column_to_track))

    client = InfluxDBClient(url=influxdb_url, token=token, org=org)
    write_to_influxdb(heavy_hitters, client)

    # Save heavy hitters data to CSV
    save_to_csv(heavy_hitters, epoch_id)

    print(f"Epoch {epoch_id}: Heavy hitters data written to InfluxDB and saved to CSV")

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn:
        kafka_df = connect_to_kafka(spark_conn)
        if kafka_df:
            kafka_df = kafka_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
            heavy_hitters_sketch = HeavyHittersSketch(threshold=100)
            
            query = kafka_df \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .trigger(processingTime='30 seconds') \
                .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, heavy_hitters_sketch)) \
                .start()
            
            query.awaitTermination()
