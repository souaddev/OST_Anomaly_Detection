from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SparkDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.master('local').getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


        self.schema = StructType([
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
        self.df = None

    def read_data_from_kafka(self):
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "swat") \
            .option("startingOffsets", "earliest") \
            .load()

        df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), self.schema).alias('data')).select("data.*")

        constant_columns = ['P202', 'P401', 'P404', 'P502', 'P601', 'P603']
        self.df = df.select([col for col in df.columns if col not in constant_columns])

    def get_processed_data(self):
        return self.df

    def process_data(self):
        if self.df is None:
            self.read_data_from_kafka()
        # Perform further processing or return the processed data
        # Example: Perform additional transformations or return the processed data
        return self.df  # Adjust the return statement based on your processing

    def stop_spark_session(self):
        self.spark.stop()  # Stop the Spark session when done