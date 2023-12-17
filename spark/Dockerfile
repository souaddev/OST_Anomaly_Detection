FROM jupyter/pyspark-notebook
COPY . .
# Install numpy using pip
RUN python3 -m pip install numpy
RUN python3 -m pip install cassandra-driver
RUN python3 -m pip install tensorflow
RUN python3 -m pip install pandas
RUN python3 -m pip install kafka-python
RUN python3 -m pip install scikit-learn
RUN python3 -m pip install stumpy
RUN python3 -m pip install influxdb-client