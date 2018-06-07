from influxdb import DataFrameClient
from influxdb import InfluxDBClient

client = DataFrameClient(host='localhost',port=8086,database='crypto_analyzer')

