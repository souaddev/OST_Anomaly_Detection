import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from hyperloglog import HyperLogLog

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("HyperLogLog") \
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
def create_selection_df_from_kafka(spark_df):

    schema = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("FIT101", StringType(), True),
    StructField("LIT101", StringType(), True),
    StructField("MV101", StringType(), True),
    StructField("P101", StringType(), True),
    StructField("P102", StringType(), True),
    StructField("AIT201", StringType(), True),
    StructField("AIT202", StringType(), True),
    StructField("AIT203", StringType(), True),
    StructField("FIT201", StringType(), True),
    StructField("MV201", StringType(), True),
    StructField("P201", StringType(), True),
    StructField("P202", StringType(), True),
    StructField("P203", StringType(), True),
    StructField("P204", StringType(), True),
    StructField("P205", StringType(), True),
    StructField("P206", StringType(), True),
    StructField("DPIT301", StringType(), True),
    StructField("FIT301", StringType(), True),
    StructField("LIT301", StringType(), True),
    StructField("MV301", StringType(), True),
    StructField("MV302", StringType(), True),
    StructField("MV303", StringType(), True),
    StructField("MV304", StringType(), True),
    StructField("P301", StringType(), True),
    StructField("P302", StringType(), True),
    StructField("AIT401", StringType(), True),
    StructField("AIT402", StringType(), True),
    StructField("FIT401", StringType(), True),
    StructField("LIT401", StringType(), True),
    StructField("P401", StringType(), True),
    StructField("P402", StringType(), True),
    StructField("P403", StringType(), True),
    StructField("P404", StringType(), True),
    StructField("UV401", StringType(), True),
    StructField("AIT501", StringType(), True),
    StructField("AIT502", StringType(), True),
    StructField("AIT503", StringType(), True),
    StructField("AIT504", StringType(), True),
    StructField("FIT501", StringType(), True),
    StructField("FIT502", StringType(), True),
    StructField("FIT503", StringType(), True),
    StructField("FIT504", StringType(), True),
    StructField("P501", StringType(), True),
    StructField("P502", StringType(), True),
    StructField("PIT501", StringType(), True),
    StructField("PIT502", StringType(), True),
    StructField("PIT503", StringType(), True),
    StructField("FIT601", StringType(), True),
    StructField("P601", StringType(), True),
    StructField("P602", StringType(), True),
    StructField("P603", StringType(), True),
    StructField("Normal/Attack", StringType(), True)
])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

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
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /sparkScripts/sketches/hyperloglog_sketch.py