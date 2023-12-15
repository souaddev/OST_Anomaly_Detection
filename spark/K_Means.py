import pandas as pd
import numpy as np
import joblib
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from pyspark.sql import SparkSession
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Initialize SparkSession
spark = SparkSession.builder.appName("LoadPreprocessedData").getOrCreate()

# Load the saved Parquet data into a PySpark DataFrame
swat_transformed = spark.read.parquet("final_preprocessed_data.parquet")
swat_transformed.printSchema()
swat_transformed.show(5)
transformed_data_pd = swat_transformed.toPandas()
# Preprocess data (e.g., normalization)
scaler = StandardScaler()
scaled_data = scaler.fit_transform(transformed_data_pd.drop(columns=['Timestamp', 'Normal/Attack']))

# Apply K-Means
kmeans = KMeans(n_clusters=5, random_state=42)  # Adjust n_clusters as needed
kmeans.fit(scaled_data)

# Get cluster centers and labels
centers = kmeans.cluster_centers_
labels = kmeans.labels_

# Calculate the distance of each point from its cluster centroid
distances = np.sqrt(np.sum((scaled_data - centers[labels])**2, axis=1))

# Determine a threshold for flagging anomalies
threshold = np.percentile(distances, 95)  # Flagging the top 5% farthest points as anomalies

# Flag anomalies and assign cluster labels
transformed_data_pd['anomaly'] = distances > threshold
transformed_data_pd['cluster'] = labels
transformed_data_pd['distance'] = distances

# Save the K-Means model
joblib.dump(kmeans, 'kmeans_model.pkl')

# InfluxDB Configuration
influxdb_url = 'http://localhost:8086'
token = '__X7OLddFCsUSr6rQh1zl5AZx45FGw_bUDUWGg8VvYNQB2uyaswDL7w8B5czMfOLCM8r2YZqEkpZ4thWaJ0maA=='
org = 'OST'
bucket = 'a6334116a243e50e'

# Function to write data to InfluxDB
def write_to_influxdb(data, client):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    for index, row in data.iterrows():
        point = Point("KMeansClustering") \
                .field("cluster", int(row['cluster'])) \
                .field("distance", float(row['distance'])) \
                .field("anomaly", int(row['anomaly'])) \
                .time(datetime.utcnow(), WritePrecision.NS)
        write_api.write(bucket, org, point)

# Initialize InfluxDB Client and write data
client = InfluxDBClient(url=influxdb_url, token=token, org=org)
write_to_influxdb(transformed_data_pd, client)

# Visualization and Analysis
sns.countplot(x=transformed_data_pd['cluster'])
plt.title('Distribution of Points Among Clusters')
plt.show()

sns.histplot(transformed_data_pd['distance'], bins=30, kde=True)
plt.title('Distribution of Distances from Cluster Centers')
plt.show()

sns.histplot(transformed_data_pd[transformed_data_pd['anomaly']]['distance'], bins=30, kde=True)
plt.title('Distribution of Distances of Anomalies from Cluster Centers')
plt.show()

# with open('./models/kmeans_model.pkl', 'wb') as f:
#             pickle.dump(K_Means, f)