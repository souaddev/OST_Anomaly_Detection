from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import WritePrecision

class InfluxDBWriter:
    def __init__(self, url="http://influxdb:8086", token="vpx6nPEOvqn_KCfNKjGyQhgwixvlnE8hG7E7Mnj_stLRGe22djhaIBXEuXx3wKv_lrDNTh39JCZ4Qqw4-cgxMQ==", org="swat_org", bucket="anomalyDetection"):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org, debug=False)
            # Create a writer API
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        except Exception as e:
            print(f"Error initializing InfluxDB client: {e}")

    def write_to_influxdb(self, points):
        try:
            # Write data to InfluxDB
            self.write_api.write(bucket=self.bucket, record=points,  write_precision=WritePrecision.MS)
           # line_protocol = points.to_line_protocol()
            #print(line_protocol)
        except Exception as e:
            print(f"Error writing data to InfluxDB: {e}")

