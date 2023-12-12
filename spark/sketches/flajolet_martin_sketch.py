import logging
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from flajolet_martin import FlajoletMartinSketch

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("FlajoletMartinSketch") \
            .getOrCreate()
        s_conn.conf.set("spark.executor.memory", "2g")
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
    return s_conn

def connect_to_kafka(spark):
    spark_df = None
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
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
    return spark_df

def process_batch(batch_df, epoch_id, flajolet_martin):
    # DPIT301:  might be useful for detecting anomalies in pressure systems.
    column_to_track = "DPIT301"  
    items = batch_df.select(column_to_track).rdd.flatMap(lambda x: x).collect()
    for item in items:
        flajolet_martin.add(item)
    
    estimated_count = flajolet_martin.estimate_count()
    print(f"Epoch {epoch_id}: Estimated distinct count for {column_to_track} = {estimated_count}")

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    
    if spark_conn:
        kafka_df = connect_to_kafka(spark_conn)
        if kafka_df:
            kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
            flajolet_martin = FlajoletMartinSketch()
            
            query = kafka_df \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .trigger(processingTime='30 seconds') \
                .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, flajolet_martin)) \
                .start()
            
            query.awaitTermination()
