# Comparative Analysis of Unsupervised Learning Methods for Real-time Anomaly Detection in Industrial Control Systems (ICS)

This project is a collaborative assignment for the Open Source Technologies / Stream Mining subjects.

## Purpose

The primary objective of this project is to conduct a comparative analysis of unsupervised learning methods for real-time anomaly detection within Industrial Control Systems (ICSs). The project aims to assess algorithmic effectiveness and applicability in domains like cybersecurity and industrial monitoring.

## Architecture

The project follows a structured architecture:

![Architecture Diagram](https://github.com/souaddev/OST_Anomaly_Detection/blob/dcc92d689bfb527cc94242c20d63140ebc03381b/documents/SystemArchitecture.png)


## Technologies Used

The project leverages several open-source technologies:

- **Kafka**: An open-source  distributed event streaming platform crucial for efficient data pipelines and streaming analytics.
- **PySpark**: An interface for Apache Spark in Python, supporting various Spark functionalities like Spark SQL, DataFrame, Streaming, MLlib (Machine Learning), and Spark Core.
- **InfluxDB**: An open-source time series database by InfluxData, suitable for storing and retrieving time series data in real-time applications.
- **Grafana**: An open-source analytics and visualization web application capable of generating charts, graphs, and alerts when connected to compatible data sources.
- **Chronograf**: A web application developed by InfluxData as part of the InfluxDB project, facilitating data visualization and exploration.
## Run the Project

To execute the project, use the following command in the terminal:

1. **Run Docker Compose:**
    ```sh
    docker-compose up 
    ```

2. **Merge SWaT Normal and Attack Datasets:**
    Run `mergeDataset.py` to merge SWaT normal and attack datasets. This script combines these datasets for further processing.

3. **Create 'swat' Topic in Kafka:**
    Inside the `kafka` folder, execute `topic_creation.ipynb` to create the 'swat' topic within Kafka. This step is required only once initially.

## Data Streaming and Processing:
4. **Data Streaming to Kafka:**
    Execute `kafka_producer.ipynb` in the `kafka` folder. This notebook streams data from CSV files into Kafka.

5. **Preprocess Data using Spark:**
    a. Find the Spark container ID:
        ```sh
        docker ps  # Copy the Spark container ID
        ```
    b. Access the Spark container:
        ```sh
        docker exec -it [spark_container_id] bash
        ```
    c. Preprocess data using Spark:
        ```sh
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /sparkScripts/preprocessing.py
        ```

6. **Perform Batch Data Preprocessing:**
    Use `batch_preprocessing.py` to preprocess data using Spark libraries.

7. **Initiate Model Training on Batch Data:**
    Commence model training by executing `<model_name>.py` scripts.

## Visualization and Result Interpretation:
8. **Configure InfluxDB Data Source in Grafana and Chronograf:**
    Set up a new InfluxDB data source in both Grafana and Chronograf to visualize and analyze the results.

## Additional Notes:
- All related scripts and sketches are located in the `spark` folder.
- To run Spark scripts, access the Spark Docker container and execute:
    ```sh
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /sparkScripts/<sketch_name.py>
    ```