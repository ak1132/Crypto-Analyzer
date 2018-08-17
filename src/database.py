'''
Basics of InfluxDB

Measurement is equivalent to table in SQL
Tag is a key-value pair of columns that get indexed by the database
Field is a key value pair of columns that do not get indexed by the database

Created on:  08/10/2018
Author:  Amogh Kulkarni
'''
import os
import configparser
from influxdb import DataFrameClient
from singleton import Singleton

configParser = configparser.ConfigParser()
configParser.read(os.curdir + r'\\resources\\config.ini')


class DbClient(metaclass=Singleton):

    def __init__(self, database=None, host=None, port=None):
        if database is None:
            self.database = configParser['database']['name']
        else:
            self.database = database

        if host is None:
            self.host = configParser['database']['host']
        else:
            self.host = host

        if port is None:
            self.port = configParser['database']['port']
        else:
            self.port = port

        self._instance = DataFrameClient(
            host=self.host, port=self.port, database=self.database)

    def save_to_db(self, df, measurement, tags=None):
        """ Saving dataframe to influx db """
        if tags is None:
            print("Write DataFrame")
            self._instance.write_points(
                df, database=self.database, measurement=measurement, protocol='json')
        else:
            print("Write DataFrame with Tags")
            self._instance.write_points(
                df, database=self.database, measurement=measurement, tags=tags, protocol='json')

    def fetch_from_db(self, query):
        """ Fetching data from influx db """

        print("Read from influx db")
        return self._instance.query(query)

    def create_db(self):
        """ Creating the influx db database """

        print("Create influx db")
        self._instance.create_database('crypto_analyzer')

    def drop_db(self):
        """ Dropping the influx db database """

        print("Influx database with all measurements")
        self._instance.drop_database(self.database)

    def df_int_to_float(self, df):
        """ Converting the int data type columns to float """

        for i in df.select_dtypes('int64').columns.values:
            df[i] = df[i].astype(float)
        return df

    def is_existing(self):
        """ Checks if database already exists """
        result = self._instance.get_list_database()
        return result is not None or len(result) > 0
