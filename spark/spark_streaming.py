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

def insert_data(session, **kwargs):
    print("inserting data...")

    Timestamp = kwargs.get('Timestamp')
    FIT101 = kwargs.get('FIT101')
    LIT101 = kwargs.get('LIT101')
    MV101 = kwargs.get('MV101')
    P101 = kwargs.get('P101')
    P102 = kwargs.get('P102')
    AIT201 = kwargs.get('AIT201')
    AIT202 = kwargs.get('AIT202')
    AIT203 = kwargs.get('AIT203')
    FIT201 = kwargs.get('FIT201')
    MV201 = kwargs.get('MV201')
    P201 = kwargs.get('P201')
    P202 = kwargs.get('P202')
    P203 = kwargs.get('P203')
    P204 = kwargs.get('P204')
    P205 = kwargs.get('P205')
    P206 = kwargs.get('P206')
    DPIT301 = kwargs.get('DPIT301')
    FIT301 = kwargs.get('FIT301')
    LIT301 = kwargs.get('LIT301')
    MV301 = kwargs.get('MV301')
    MV302 = kwargs.get('MV302')
    MV303 = kwargs.get('MV303')
    MV304 = kwargs.get('MV304')
    P301 = kwargs.get('P301')
    P302 = kwargs.get('P302')
    AIT401 = kwargs.get('AIT401')
    AIT402 = kwargs.get('AIT402')
    FIT401 = kwargs.get('FIT401')
    LIT401 = kwargs.get('LIT401')
    P401 = kwargs.get('P401')
    P402 = kwargs.get('P402')
    P403 = kwargs.get('P403')
    P404 = kwargs.get('P404')
    UV401 = kwargs.get('UV401')
    AIT501 = kwargs.get('AIT501')
    AIT502 = kwargs.get('AIT502')
    AIT503 = kwargs.get('AIT503')
    AIT504 = kwargs.get('AIT504')
    FIT501 = kwargs.get('FIT501')
    FIT502 = kwargs.get('FIT502')
    FIT503 = kwargs.get('FIT503')
    FIT504 = kwargs.get('FIT504')
    P501 = kwargs.get('P501')
    P502 = kwargs.get('P502')
    PIT501 = kwargs.get('PIT501')
    PIT502 = kwargs.get('PIT502')
    PIT503 = kwargs.get('PIT503')
    FIT601 = kwargs.get('FIT601')
    P601 = kwargs.get('P601')
    P602 = kwargs.get('P602')
    P603 = kwargs.get('P603')
    Normal_Attack = kwargs.get('Normal_Attack')

    try:
        session.execute("""
            INSERT INTO swat_spark_streams.processed_batch(Timestamp, FIT101, LIT101, MV101, P101, P102, AIT201, AIT202, AIT203,
                FIT201, MV201, P201, P202, P203, P204, P205, P206, DPIT301, FIT301, LIT301, MV301, MV302, MV303, MV304, P301, P302,
                AIT401, AIT402, FIT401, LIT401, P401, P402, P403, P404, UV401, AIT501, AIT502, AIT503, AIT504, FIT501, FIT502, FIT503,
                FIT504, P501, P502, PIT501, PIT502, PIT503, FIT601, P601, P602, P603, Normal_Attack)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (Timestamp, FIT101, LIT101, MV101, P101, P102, AIT201, AIT202, AIT203, FIT201, MV201, P201, P202, P203, P204, P205, P206,
              DPIT301, FIT301, LIT301, MV301, MV302, MV303, MV304, P301, P302, AIT401, AIT402, FIT401, LIT401, P401, P402, P403, P404,
              UV401, AIT501, AIT502, AIT503, AIT504, FIT501, FIT502, FIT503, FIT504, P501, P502, PIT501, PIT502, PIT503, FIT601, P601, P602,
              P603, Normal_Attack))

        logging.info(f"Data inserted for {Timestamp}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

# insert processed data into Cassandra.

def insert_into_cassandra(spark_df):
    # Insert into Cassandra
    spark_df.write.format("org.apache.spark.sql.cassandra") \
        .option('checkpointLocation', './spark/checkpoint') \
        .option('keyspace', 'swat_spark_streams') \
        .option('table', 'processed_batch') \
        .mode("append") \
        .save()



def create_spark_connection():
    s_conn = None

    try:
        #.config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,"
            #                               "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        s_conn = SparkSession.builder \
            .master('local') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        s_conn.conf.set("spark.kafka.consumer.max.poll.interval.ms", "300000")  # Adjust the value as needed
        s_conn.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark):
    spark_df = None
    #if you wanna run it in the container please change the address to kafka:29092
    try:
        spark_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
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


def schema_mapping(spark_df):
    schema_mapping = {
        "timestamp": "Timestamp",
        "fit101": "double",
        "lit101": "double",
        "mv101": "double",
        "p101": "double",
        "p102": "double",
        "ait201": "double",
        "ait202": "double",
        "ait203": "double",
        "fit201": "double",
        "mv201": "double",
        "p201": "double",
        "p202": "double",
        "p203": "double",
        "p204": "double",
        "p205": "double",
        "p206": "double",
        "dpit301": "double",
        "fit301": "double",
        "lit301": "double",
        "mv301": "double",
        "mv302": "double",
        "mv303": "double",
        "mv304": "double",
        "p301": "double",
        "p302": "double",
        "ait401": "double",
        "ait402": "double",
        "fit401": "double",
        "lit401": "double",
        "p401": "double",
        "p402": "double",
        "p403": "double",
        "p404": "double",
        "uv401": "double",
        "ait501": "double",
        "ait502": "double",
        "ait503": "double",
        "ait504": "double",
        "fit501": "double",
        "fit502": "double",
        "fit503": "double",
        "fit504": "double",
        "p501": "double",
        "p502": "double",
        "pit501": "double",
        "pit502": "double",
        "pit503": "double",
        "fit601": "double",
        "p601": "double",
        "p602": "double",
        "p603": "double",
        "normal_attack": "double"
    }
    
    for col_name, col_type in schema_mapping.items():
        # Replace null values with a default value (you can adjust this as needed)
        default_value = 0.0  # You can change this to a different default value if needed
        spark_df = spark_df.withColumn(col_name, col(col_name).cast(col_type).coalesce(default_value))
    
    return spark_df


def to_lower(spark_df):
    columns_lower = [col(col_name).alias(col_name.lower()) for col_name in spark_df.columns]
    return spark_df.select(*columns_lower)



# Define a simple foreachBatch function
def process_batch(spark_df, epoch_id):

    # Drop duplicates and missing values
    spark_df = spark_df.dropDuplicates().na.drop()

    # Convert "Normal/Attack/A ttack" column to binary
    spark_df = spark_df.withColumn("normal_attack", when((spark_df["normal_attack"] == "Attack") | (spark_df["normal_attack"] == "A ttack") , 1).otherwise(0))

    # Convert Timestamp to TimestampType
    # spark_df = spark_df.withColumn("Timestamp", unix_timestamp(col("Timestamp"), "dd/MM/yyyy h:mm:ss a").cast(TimestampType())) 
    spark_df = spark_df.withColumn('Timestamp', to_timestamp('Timestamp', ' dd/MM/yyyy hh:mm:ss a'))

    # Insert into Cassandra using the separate function
    spark_df = to_lower(spark_df)
    insert_into_cassandra(spark_df)

    spark_df.show()

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /sparkScripts/spark_streaming_001.py
if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        df = connect_to_kafka(spark_conn)
        if df is None: 
            print("df is none")
        selection_df = create_selection_df_from_kafka(df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

        logging.info("Streaming is being started...")
        selection_df = selection_df.withColumnRenamed("Normal/Attack", "normal_attack")
        #selection_df = schema_mapping(selection_df)
        selection_df.printSchema()
        
        query = selection_df.writeStream.outputMode("append").format("console") \
                            .trigger(processingTime='30 seconds') \
                            .foreachBatch(process_batch) \
                            .start()
 
        query.awaitTermination()


#spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /sparkScripts/spark_streaming_001.py
# docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
