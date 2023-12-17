from spark_processor import SparkDataProcessor
from flajolet_martin2 import FlajoletMartinSketch

# Initialize SparkDataProcessor and process the data
data_processor = SparkDataProcessor()
data_processor.read_data_from_kafka()
df = data_processor.get_processed_data()

# Flajolet-Martin sketches storage
fm_sketches = {}

def process_batch(batch_df, epoch_id):
    global fm_sketches

    # Initialize Flajolet-Martin sketch for this batch
    fm_sketch = FlajoletMartinSketch(num_perm=128)

    # Extract values from DataFrame columns
    # Choose columns with expected high cardinality
    column_to_track = "DPIT301"  
    column_values = batch_df.select(column_to_track).rdd.flatMap(lambda x: x).collect()
    for value in column_values:
            fm_sketch.add_value(value)

 # Get the Flajolet-Martin sketch for the current batch and estimate count
    current_sketch = fm_sketch.get_sketch()
    estimated_count = fm_sketch.estimate_count()
    print(f"Epoch {epoch_id}: Estimated distinct count for {column_to_track} = {estimated_count}")
    # Store the current sketch for future use or comparison
    fm_sketches[epoch_id] = current_sketch

# Define the streaming query using foreachBatch
query = df.writeStream.foreachBatch(process_batch).start()
query.awaitTermination()

# Don't forget to stop the Spark session when done
# data_processor.stop_spark_session()
