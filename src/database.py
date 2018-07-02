from influxdb import DataFrameClient
import pandas as pd

'''
Basics of InfluxDB

Measurement is equivalent to table in SQL
Tag is a key-value pair of columns that get indexed by the database
Field is a key value pair of columns that do not get indexed by the database
'''


class DbClient:

    def __init__(self, database, host, port):
        self.database = database
        self.client = DataFrameClient(host=host, port=port, database=database)

    def save_to_db(self, df):
        print("Write DataFrame")
        self.client.write_points(df, database=self.database, protocol='json')
        print("Write DataFrame with Tags")
        self.client.write_points(df, database=self.database,
                                 tags={'k1': 'v1', 'k2': 'v2'}, protocol='json')

    def fetch_from_db(self, query):
        print("Read DataFrame")
        return self.client.query("select * from demo")

    def drop_db(self):
        self.client.drop_database(self.database)
