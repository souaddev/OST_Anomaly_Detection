import logging
import json
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spacesaving import SpaceSaving 

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .master('local') \
            .getOrCreate()
        s_conn.conf.set("spark.kafka.consumer.max.poll.interval.ms", "300000")  # Adjust the value as needed

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark):
    spark_df = None
    try:
        spark_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "swat_sketch") \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 50) \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
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

# Define foreachBatch function
def process_batch(spark_df, epoch_id, space_saving):

    # Drop duplicates and missing values
    spark_df = spark_df.dropDuplicates().na.drop()

    # Convert "Normal/Attack/A ttack" column to binary
    spark_df = spark_df.withColumn("Normal/Attack", when((spark_df["Normal/Attack"] == "Attack") | (spark_df["Normal/Attack"] == "A ttack") , 1).otherwise(0))

    # Convert Timestamp to TimestampType
    # spark_df = spark_df.withColumn("Timestamp", unix_timestamp(col("Timestamp"), "dd/MM/yyyy h:mm:ss a").cast(TimestampType())) 

    # Specify columns to track with SpaceSaving
    # columns_to_track = [
    #     'Normal/Attack', 'MV101', 'P101', 'P102', 'MV201', 'P201', 'P203',
    #     'P204', 'P205', 'P206', 'MV301', 'MV302', 'MV303', 'MV304', 'P301',
    #     'P302', 'P402', 'P403', 'UV401', 'P501', 'P602'
    # ]

    columns_to_track = [
        'Normal/Attack', 'MV101', 'P101', 'P102'
    ]

    # Update SpaceSaving instance with items from the batch for specified columns
    for column_name in columns_to_track:
        items = spark_df.select(column_name).rdd.flatMap(lambda x: x).collect()
        for item in items:
            space_saving.update(item)

    # Print the epoch_id along with other information
    print(f"Processing batch for epoch_id: {epoch_id}")
    heavy_hitters = space_saving.query()
    print("Heavy Hitters:", heavy_hitters)

    #spark_df.show()


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        df = connect_to_kafka(spark_conn)
        if df is None: 
            print("df is none")
        # Initialize SpaceSaving with k value (adjust as needed)
        k_value = 10  # Change this to your desired value
        space_saving = SpaceSaving(k_value)

        selection_df = create_selection_df_from_kafka(df)
        logging.info("Streaming is being started...")
        selection_df.printSchema()

        query = selection_df.writeStream.outputMode("append").format("console") \
            .trigger(processingTime='30 seconds') \
            .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, space_saving)) \
            .start()

        query.awaitTermination()


#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /sparkScripts/sketches/space_saving_sketch.py