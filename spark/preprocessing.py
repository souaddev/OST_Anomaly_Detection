import pyspark
import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col, lit,row_number
from pyspark.sql.window import Window
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.ml.feature import MinMaxScaler, VectorAssembler


influxdb_url = 'http://influxdb:8086'
token = "n2JFLRm9VaWepZ4zwk8YJlhR5AVEa_WIk2KCn2xhM9cMYW5s8qeMH5r0yX9wmOdyEs_Nq5Evnpwpm97sPdfP5A=="
#token = "n2JFLRm9VaWepZ4zwk8YJlhR5AVEa_WIk2KCn2xhM9cMYW5s8qeMH5r0yX9wmOdyEs_Nq5Evnpwpm97sPdfP5A=="
org = "dataCrew"
bucket_ = "anomalyDetection"
measurement = 'isopredictions'


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

spark = SparkSession.builder.master('local').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "swat_203") \
        .option("startingOffsets", "earliest") \
        .load()
   

df = df.withColumn("value", df["value"].cast(StringType()))

df = df.withColumn("data", from_json(col("value"), schema)).select("data.*")

schema_mapping = {"Timestamp": "string", "FIT101": "double", "LIT101": "double", "MV101": "double", "P101": "double", "P102": "double", 
                  "AIT201": "double", "AIT202": "double", "AIT203": "double", "FIT201": "double", "MV201": "double", "P201": "double", 
                  "P202": "double", "P203": "double", "P204": "double", "P205": "double", "P206": "double", "DPIT301": "double", "FIT301": "double", 
                  "LIT301": "double", "MV301": "double", "MV302": "double", "MV303": "double", "MV304": "double", "P301": "double", "P302": "double", 
                  "AIT401": "double", "AIT402": "double", "FIT401": "double", "LIT401": "double", "P401": "double", "P402": "double", "P403": "double", 
                  "P404": "double", "UV401": "double", "AIT501": "double", "AIT502": "double", "AIT503": "double", "AIT504": "double", "FIT501": "double", 
                  "FIT502": "double", "FIT503": "double", "FIT504": "double", "P501": "double", "P502": "double", "PIT501": "double", "PIT502": "double", 
                  "PIT503": "double", "FIT601": "double", "P601": "double", "P602": "double", "P603": "double", "Normal/Attack": "string"}

for col_name, col_type in schema_mapping.items():
    df = df.withColumn(col_name, df[col_name].cast(col_type))
 
#Start preprocessing steps using Spark functions
# Drop constant columns from the DataFrame
constant_columns = ['P202', 'P401', 'P404', 'P502', 'P601', 'P603']
df = df.select([col for col in df.columns if col not in constant_columns])

# Drop duplicates and missing values
df = df.dropDuplicates().na.drop()

# Convert "Normal/Attack" column to binary
df = df.withColumn("Normal/Attack", when(df["Normal/Attack"] == "Attack", 1).otherwise(0))

# Separate numerical and categorical features
numerical_features = ['FIT101', 'LIT101', 'AIT201', 'AIT202', 'AIT203', 'FIT201', 'DPIT301', 'FIT301', 'LIT301', 'AIT401', 'AIT402', 'FIT401', 'LIT401', 
                      'AIT501', 'AIT502', 'AIT503', 'AIT504', 'FIT501', 'FIT502', 'FIT503', 'FIT504', 'PIT501', 'PIT502', 'PIT503', 'FIT601']
categorical_features = ['MV101', 'P101', 'P102', 'MV201', 'P201', 'P203', 'P204', 'P205', 'P206', 'MV301', 'MV302', 'MV303', 'MV304', 'P301', 'P302',
                         'P402', 'P403', 'UV401', 'P501', 'P602']

# Load the swat dataset for min-max scaling
swat_df = spark.read.csv("/sparkScripts/dataset/SWat_merged.csv", header=True, inferSchema=True)
min_max_values = swat_df.select([
    min(col(col_name)).alias(f"min_{col_name}") for col_name in numerical_features
] + [
    max(col(col_name)).alias(f"max_{col_name}") for col_name in numerical_features
]).collect()[0]

min_max_dict = min_max_values.asDict()
broadcast_min_max = spark.sparkContext.broadcast(min_max_dict)

# Apply Min-Max Scaling and One-Hot Encoding function
def min_max_scaling_and_encoding(batch_df, epoch_id):
    print('==> Starting Min-Max Scaling and oneHotEncoder > ')
    for col_name in numerical_features:
        min_val = broadcast_min_max.value[f"min_{col_name}"]
        max_val = broadcast_min_max.value[f"max_{col_name}"]
        batch_df = batch_df.withColumn(col_name, (col(col_name) - min_val) / (max_val - min_val))

    encoder = OneHotEncoder(inputCols=categorical_features, outputCols=[f"{col}_encoded" for col in categorical_features])
    encoder_model = encoder.fit(batch_df)
    batch_dff = encoder_model.transform(batch_df)

    return batch_df


#Isolation forest for anomaly detetion 
def predict_anomalies(df, epoch_id):
    batch_df = min_max_scaling_and_encoding(df,epoch_id)

    batch_df = batch_df.drop('Timestamp').drop('Normal/Attack')
    print('==> Result of preprocessing > ')
    batch_df.show(2)

   # Load the Isolation Forest model
    print('Loading Isolation Forest')
    isolation_forest_model = joblib.load('/sparkScripts/models/isolation_forest.pkl')

    #Predict
    real_time_testing_data=batch_df.toPandas()
    predictions = isolation_forest_model.predict(real_time_testing_data)
    print('This is the predictions :')
    print(predictions)

    # original DataFrame + Prediction
    df_with_predictions = add_anomaly_prediction_column(df, predictions,'ISO_FOREST_prediction')
    print('Show only two rows...')
    df_with_predictions.show(2)

    # Send data to influxDB

    send_to_influxdb(df_with_predictions, bucket_, token)
    print('Prediction Data saved to InfluxDB successfully... !  Smile :) ')

def add_anomaly_prediction_column(df, predictions, col_name='anomaly_prediction'):
     # Add a unique index to the DataFrame based on 'Timestamp'
    window_spec = Window.orderBy("Timestamp")
    df_with_index = df.withColumn("index", row_number().over(window_spec))

    # Convert the numpy array to a list
    predictions_list = predictions.tolist()

    # Convert -1 to 1 and 1 to 0 in the predictions
    converted_predictions = [1 if val == -1 else 0 for val in predictions_list]

    # Create a Pandas DataFrame from the given DataFrame
    pandas_df = df_with_index.select("*").toPandas()

    # Add the converted predictions as a new column to the Pandas DataFrame
    pandas_df[col_name] = None
    for i, row in pandas_df.iterrows():
        idx = i % len(converted_predictions)
        pandas_df.at[i, col_name] = converted_predictions[idx]

    # Convert the Pandas DataFrame back to a Spark DataFrame
    df_with_predictions = spark.createDataFrame(pandas_df)

    return df_with_predictions



def send_to_influxdb(df, bucket_, token):
    
    # Create an InfluxDB client
    client = InfluxDBClient(url=influxdb_url, token=token, org=org)

    # Create a write API
    write_api = client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000))

    # Convert DataFrame to InfluxDB Point format
    data_points = [Point(measurement).field(col, value) for row in df.collect() for col, value in row.asDict().items() if col != 'Timestamp']

    # Write data to InfluxDB
    write_api.write(bucket=bucket_, org=org, record=data_points)

    # Close the InfluxDB client
    client.close()


# Apply scaling and encoding using foreachBatch
#query = df.writeStream.foreachBatch(min_max_scaling_and_encoding).outputMode("update").start()
# Define your streaming query with trigger and foreachBatch
# query = df.writeStream \
#     .foreachBatch(min_max_scaling_and_encoding) \
#     .outputMode("update") \
#     .format("console") \
#     .trigger(processingTime='120 seconds') \
#     .start()
query = df.writeStream.foreachBatch(predict_anomalies).outputMode("update").start()
query.awaitTermination()

    
    #v spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /sparkScripts/preprocessing.py