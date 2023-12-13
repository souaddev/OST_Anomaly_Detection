import logging
from pyspark.sql import SparkSession
from hyperloglog import HyperLogLog

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

def process_batch(batch_df, epoch_id, hll_dict):
    columns_to_track = ["AIT201", "AIT202", "AIT203", "AIT401", "AIT402", "AIT501", "AIT502", "AIT503", "AIT504"]
    for column in columns_to_track:
        items = batch_df.select(column).rdd.flatMap(lambda x: x).collect()
        for item in items:
            hll_dict[column].add(item)
        estimated_count = hll_dict[column].count()
        print(f"Epoch {epoch_id}: Estimated distinct count for {column} = {estimated_count}")

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    
    if spark_conn:
        kafka_df = connect_to_kafka(spark_conn)
        if kafka_df:
            kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

            # Initialize a HyperLogLog object for each column
            hll_dict = {col: HyperLogLog() for col in ["AIT201", "AIT202", "AIT203", "AIT401", "AIT402", "AIT501", "AIT502", "AIT503", "AIT504"]}
            
            query = kafka_df \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .trigger(processingTime='30 seconds') \
                .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, hll_dict)) \
                .start()

            query.awaitTermination()
