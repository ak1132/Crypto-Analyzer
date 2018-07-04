'''
Basics of InfluxDB

Measurement is equivalent to table in SQL
Tag is a key-value pair of columns that get indexed by the database
Field is a key value pair of columns that do not get indexed by the database
'''
from influxdb import DataFrameClient
import configparser
import os

configParser = configparser.ConfigParser()
configParser.read(os.curdir + '\\resources\config.ini')


class DbClient:

    def __init__(self, database=None, host=None, port=None):

        if database is None:
            self.database = configParser['database']['name']
        else:
            self.database = database

        if host is None:
            self.host = configParser.get('database', 'host')
        else:
            self.host = host

        if port is None:
            self.port = configParser.get('database', 'port')
        else:
            self.port = database

        self.client = DataFrameClient(host=self.host, port=self.port, database=self.database)

    def save_to_db(self, df, measurement, tags=None):

        if tags is None:
            print("Write DataFrame")
            self.client.write_points(df, database=self.database, measurement=measurement, protocol='json')
        else:
            print("Write DataFrame with Tags")
            self.client.write_points(df, database=self.database, measurement=measurement, tags=tags, protocol='json')

    def fetch_from_db(self, query):
        print("Read DataFrame")
        return self.client.query(query)

    def create_db(self):
        self.client.create_database('crypto_analyzer')

    def drop_db(self):
        self.client.drop_database(self.database)

    def is_existing(self):
        result = self.client.get_list_database()
        return result is not None or len(result) > 0
