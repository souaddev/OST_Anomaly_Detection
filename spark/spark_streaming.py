import logging
import json
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import cassandra
from cassandra.cluster import Cluster
#from cassandra.auth import PlainTextAuthProvider

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS swat_spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS swat_spark_streams.processed_batch (
        Timestamp TIMESTAMP PRIMARY KEY,
        FIT101 DOUBLE,
        LIT101 DOUBLE,
        MV101 DOUBLE,
        P101 DOUBLE,
        P102 DOUBLE,
        AIT201 DOUBLE,
        AIT202 DOUBLE,
        AIT203 DOUBLE,
        FIT201 DOUBLE,
        MV201 DOUBLE,
        P201 DOUBLE,
        P202 DOUBLE,
        P203 DOUBLE,
        P204 DOUBLE,
        P205 DOUBLE,
        P206 DOUBLE,
        DPIT301 DOUBLE,
        FIT301 DOUBLE,
        LIT301 DOUBLE,
        MV301 DOUBLE,
        MV302 DOUBLE,
        MV303 DOUBLE,
        MV304 DOUBLE,
        P301 DOUBLE,
        P302 DOUBLE,
        AIT401 DOUBLE,
        AIT402 DOUBLE,
        FIT401 DOUBLE,
        LIT401 DOUBLE,
        P401 DOUBLE,
        P402 DOUBLE,
        P403 DOUBLE,
        P404 DOUBLE,
        UV401 DOUBLE,
        AIT501 DOUBLE,
        AIT502 DOUBLE,
        AIT503 DOUBLE,
        AIT504 DOUBLE,
        FIT501 DOUBLE,
        FIT502 DOUBLE,
        FIT503 DOUBLE,
        FIT504 DOUBLE,
        P501 DOUBLE,
        P502 DOUBLE,
        PIT501 DOUBLE,
        PIT502 DOUBLE,
        PIT503 DOUBLE,
        FIT601 DOUBLE,
        P601 DOUBLE,
        P602 DOUBLE,
        P603 DOUBLE,
        Normal_Attack DOUBLE);
    """)

    print("Table created successfully!")



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
            .option("subscribe", "swat") \
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
def process_batch(spark_df, epoch_id):

    # Drop duplicates and missing values
    spark_df = spark_df.dropDuplicates().na.drop()

    # Convert "Normal/Attack/A ttack" column to binary
    spark_df = spark_df.withColumn("Normal/Attack", when((spark_df["Normal/Attack"] == "Attack") | (spark_df["Normal/Attack"] == "A ttack") , 1).otherwise(0))

    # Convert Timestamp to TimestampType
    # spark_df = spark_df.withColumn("Timestamp", unix_timestamp(col("Timestamp"), "dd/MM/yyyy h:mm:ss a").cast(TimestampType())) 
    spark_df.show()


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        df = connect_to_kafka(spark_conn)
        if df is None: 
            print("df is none")
        selection_df = create_selection_df_from_kafka(df)
       

        logging.info("Streaming is being started...")

        selection_df.printSchema()
 
        query = selection_df.writeStream.outputMode("append").format("console") \
                            .trigger(processingTime='30 seconds') \
                            .foreachBatch(process_batch) \
                            .start()
 
        query.awaitTermination()
