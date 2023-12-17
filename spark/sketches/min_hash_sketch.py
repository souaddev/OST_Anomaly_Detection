import numpy as np
from min_hash import MinHashSketch
from spark_processor import SparkDataProcessor

# Create an instance of the SparkDataProcessor class
data_processor = SparkDataProcessor()

# Process the data
df = data_processor.process_data()

# Define a function to calculate MinHash similarity
def process_batch(batch_df, epoch_id):
    global minhash_sketch, minhash_sketches
    
    # Reset MinHash sketch for each epoch
    minhash_sketch = MinHashSketch(num_perm=128)
    
    # Extract values from DataFrame columns
    values = batch_df.select('LIT101', 'FIT101', 'MV101', 'AIT201', 'FIT201').collect()
    
    # Add values to MinHashSketch
    for row in values:
        minhash_sketch.add_values(row)
    
    # Get the MinHash sketch for the current batch
    current_sketch = minhash_sketch.get_sketch()
    
    # Calculate similarity with the previous batch
    if epoch_id > 0:
        prev_sketch = minhash_sketches[epoch_id - 1]
        
        # Calculate MinHash similarity
        similarity = np.mean(current_sketch == prev_sketch)
        
        # Calculate Jaccard similarity
        jaccard_similarity = MinHashSketch.jaccard_similarity(current_sketch, prev_sketch)
        
        # Number of Common Values between Current and Previous Sketches
        common_values_count = len(set(current_sketch).intersection(set(prev_sketch)))
        print(f"Epoch {epoch_id}: Number of common values between current and previous sketches = {common_values_count}")

        # Number of Unique Values in Current Sketch Not Present in Previous Sketch
        unique_values_current_not_in_prev = len(set(current_sketch).difference(set(prev_sketch)))
        print(f"Epoch {epoch_id}: Number of unique values in the current sketch not present in the previous sketch = {unique_values_current_not_in_prev}")

        print(f"Epoch {epoch_id}: MinHash similarity = {similarity}")
        print(f"Epoch {epoch_id}: Jaccard similarity = {jaccard_similarity}")
        print('-------------------------------------------------------------------')
    
    # Store the current sketch for the next comparison
    minhash_sketches[epoch_id] = current_sketch

minhash_sketches = {}
    # Start streaming query
query = df.writeStream.foreachBatch(process_batch).start()
query.awaitTermination()