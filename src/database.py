'''
Basics of InfluxDB

Measurement is equivalent to table in SQL
Tag is a key-value pair of columns that get indexed by the database
Field is a key value pair of columns that do not get indexed by the database

Created on:  08/10/2018
Author:  Amogh Kulkarni
'''

from influxdb import DataFrameClient
import configparser
import os
from configEngine import ConfigEngine

configParser = ConfigEngine()


class DbClient:

    class __impl:
        """ Implementation of the singleton interface """

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

            self.client = DataFrameClient(
                host=self.host, port=self.port, database=self.database)

    # storage for the instance reference
    __instance = None

    def __init__(self):
        """ Create singleton instance """

        # Check whether we already have an instance
        if DbClient.__instance is None:
            # Create and remember instance
            DbClient.__instance = DbClient.__impl()

        # Store instance reference as the only member in the handle
        self.__dict__['_Singleton__instance'] = DbClient.__instance

    def __getattr__(self, attr):
        """ Delegate access to implementation """
        return getattr(self.__instance, attr)

    def __setattr__(self, attr, value):
        """ Delegate access to implementation """
        return setattr(self.__instance, attr, value)

    def save_to_db(self, df, measurement, tags=None):
        """ Saving dataframe to influx db """
        if tags is None:
            print("Write DataFrame")
            self.client.write_points(
                df, database=self.database, measurement=measurement, protocol='json')
        else:
            print("Write DataFrame with Tags")
            self.client.write_points(
                df, database=self.database, measurement=measurement, tags=tags, protocol='json')

    def fetch_from_db(self, query):
        """ Fetching data from influx db """

        print("Read from influx db")
        return self.client.query(query)

    def create_db(self):
        """ Creating the influx db database """

        print("Create influx db")
        self.client.create_database('crypto_analyzer')

    def drop_db(self):
        """ Dropping the influx db database """

        print("Influx database with all measurements")
        self.client.drop_database(self.database)

    def df_int_to_float(self, df):
        """ Converting the int data type columns to float """

        for i in df.select_dtypes('int64').columns.values:
            df[i] = df[i].astype(float)
        return df

    def is_existing(self):
        """ Checks if database already exists """
        result = self.client.get_list_database()
        return result is not None or len(result) > 0
