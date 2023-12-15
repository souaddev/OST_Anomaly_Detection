FROM jupyter/pyspark-notebook
COPY . .
# Install numpy using pip
RUN python3 -m pip install numpy
RUN python3 -m pip install cassandra-driver
RUN python3 -m pip install kafka-python

