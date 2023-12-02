from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os
import pyspark

# Specify Kafka dependencies
spark = SparkSession.builder \
            .appName('OST2') \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
            .getOrCreate()


# Kafka configuration
kafka_config = {
    'kafka.bootstrap.servers': 'localhost:29092',  
    'subscribe': 'swat'
}


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['kafka.bootstrap.servers']) \
    .option("subscribe", kafka_config['subscribe']) \
    .load()


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

# Convert value column from binary to string
df = df.withColumn("value", df["value"].cast(StringType()))
 
# Apply from_json on the string value column using your schema
df = df.withColumn("data", from_json(col("value"), schema)).select("data.*")
 
df.printSchema()
 
query = df.writeStream.outputMode("append").format("console") \
    .trigger(processingTime='30 seconds').start()
 
query.awaitTermination()