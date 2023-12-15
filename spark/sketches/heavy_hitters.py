
import pyspark
from pyspark.sql.functions import col, count

class HeavyHittersSketch:
    def __init__(self, threshold):
        self.threshold = threshold

    def process_stream(self, data):
        # Counting the frequency of each item
        frequency_df = data.groupBy('item').agg(count('item').alias('frequency'))
        
        # Filtering items that have a frequency higher than the threshold
        heavy_hitters = frequency_df.filter(col('frequency') > self.threshold)
        return heavy_hitters
