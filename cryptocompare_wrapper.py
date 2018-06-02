#!/bin/python
#
# A Python wrapper for https://min-api.cryptocompare.com/
#
# Created on:       03/03/2018  Aditya Shirode
# Last modified:    03/03/2018  Aditya Shirode
#

import time
import yaml
import logging
import requests
import datetime
import pandas as pd

api_queries = 'api_queries.yaml'

with open(api_queries, 'r') as f:
    try:
        query_yaml = yaml.load(f)
    except yaml.YAMLError as e:
        logging.error(e)

API_ENDPOINT = query_yaml['api_endpoint']

# Defaults
CURR = 'USD'
EXCHANGE = 'CCCAGG'
COIN = 'BTC'
COIN_LIST = ['BTC', 'ETH']
EXCHANGES = ['Kucoin', 'Cryptopia', 'HitBTC']


def format_parameter(parameter):
    """ Format parameters for the query """
    if isinstance(parameter, list):
        return ','.join(parameter)
    else:
        return parameter


def get_url(query_name, **kwargs):
    """
    Get formatted url for a required query
    Pass the arguments to the query as keyword arguments
    """
    query_data = query_yaml[query_name]
    query_url = API_ENDPOINT + query_data['url']
    query_arguments = []
    # Check if all the required arguments are provided
    if 'required' in query_data['parameters'].keys() and any(argument not in kwargs for argument in query_data['parameters']['required']):
        logging.info("Not all required arguments provided for {query}. "
                     "Required arguments are {args}.".format(query=query_name, args=query_data['parameters']['required']))
        return None
    else:
        possible_query_arguments = list(query_data.get('parameters', {}).get('required', {}).keys()) + list(query_data.get('parameters', {}).get('additional', {}).keys())
        for argument, value in kwargs.items():
            if argument in possible_query_arguments:
                query_arguments.append("{argument}={value}".format(argument=argument, value=format_parameter(value)))

    query = query_url + '?' + '&'.join(query_arguments)
    return query


def query_cryptocompare(url):
    """ Query CryptoCompare API """
    try:
        response = requests.get(url).json()
    except Exception as e:
        logging.error("Failure while querying {query}. \n{err}".format(query=url, err=e))
        return None

    if not response or 'Response' not in response.keys():
        return response

    if response['Response'] is 'Error':
        logging.error("Failed to query {url}".format(url=url))
        return None
    return response


def convert_timestamp(timestamp):
    """ Convert timestamp into readable datetime """
    try:
        return datetime.datetime.fromtimestamp(int(timestamp)).strftime('%d-%m-%Y %H:%M:%S')
    except Exception as e:
        logging.debug(e)
        return None


def get_data(response):
    """ Separate query data from response """
    header = {key: (value if key != 'Data' else len(value)) for key, value in response.items()}
    data = response['Data']
    return header, data


def get_readable_df(response):
    """ Extract data from given response and return a dataframe """
    header, data = get_data(response)
    try:
        df_data = pd.DataFrame(data)
        df_data = df_data.rename(columns={'time': 'timestamp'})
        df_data['time'] = df_data.timestamp.apply(convert_timestamp)
        df_data = df_data.set_index('time')
    except AttributeError as e:
        logging.debug(e)
        return None
    return df_data


def get_coin_list():
    """ Get coin list """
    resp = query_cryptocompare(get_url('coinlist'))
    header, data = get_data(resp)
    return data

# coins = get_coin_list()
# COIN_DB = pd.DataFrame.from_dict(coins, orient='index')
# print(COIN_DB.head())


def get_exchanges_list():
    """ Get a list of all exchanges on CryptoCompare """
    data = query_cryptocompare(get_url('exchanges'))
    return data

# exchanges = get_exchanges_list()
# EXCHANGE_DB = pd.DataFrame.from_dict(e, orient='index')
# print(EXCHANGE_DB.head())


def get_price(coin, to_curr=CURR, exchange=EXCHANGE, **kwargs):
    """ Get real time price of the coin """
    if isinstance(coin, list):
        return query_cryptocompare(
            get_url(
                'pricemulti',
                fsyms=coin,
                tsyms=to_curr,
                e=exchange,
                **kwargs
            )
        )
    else:
        return query_cryptocompare(
            get_url(
                'price',
                fsym=coin,
                tsyms=to_curr,
                e=exchange,
                **kwargs
            )
        )

# print(get_price(COIN))
# print(get_price(COIN_LIST))
# print(get_price(COIN_LIST, ['USD', 'ETH', 'LTC']))


def get_historical_price_timestamp(coin, to_curr=CURR, timestamp=time.time(), exchange=EXCHANGE, **kwargs):
    """ Get value of coin in currency at a particular timestamp """
    if isinstance(timestamp, datetime.datetime):
        timestamp = time.mktime(timestamp.timetuple())

    return query_cryptocompare(
        get_url(
            'pricehistorical',
            fsym=coin,
            tsyms=to_curr,
            ts=int(timestamp),
            e=exchange,
            **kwargs
        )
    )

# print(get_historical_price_timestamp(COIN))


def get_historical_price_day(coin, to_curr=CURR, timestamp=time.time(), exchange=EXCHANGE, allData='false', **kwargs):
    """ Get price per day for the past month """
    resp = query_cryptocompare(
        get_url(
            'histoday',
            fsym=coin,
            tsym=to_curr,
            e=exchange,
            allData=allData,
            **kwargs
        )
    )
    df =  get_readable_df(resp)
    #df['coin'] = coin
    #print("Coin inserted in Dataframe")
    #df.set_index(['coin', 'time'])
    #print("Coin and Time index Set")
    return df


def get_historical_price_last_day(*args, **kwargs):
    """ Get price for last day """
    return get_historical_price_day(*args, **kwargs, limit=1)

# print(get_historical_price_day(COIN))
# print(get_historical_price_last_day(coin='ZRX', to_curr='BTC', exchange='Binance'))


def get_historical_price_day_full(*args, **kwargs):
    """ Get price per day for all time  """
    return get_historical_price_day(*args, **kwargs, allData='true')

# print(get_historical_price_day_full(COIN))


def get_historical_price_hour(coin, to_curr=CURR, exchange=EXCHANGE, limit=168, **kwargs):
    """ Get price per hour for past 7 days """
    resp = query_cryptocompare(
        get_url(
            'histohour',
            fsym=coin,
            tsym=to_curr,
            e=exchange,
            limit=limit,
            **kwargs
        )
    )
    #print(resp)
    return get_readable_df(resp)


def get_historical_price_last_hour(*args, **kwargs):
    """ Get price for the last hour """
    return get_historical_price_hour(*args, **kwargs, limit=1)

# coin_hour = get_historical_price_hour(COIN)
# print(coin_hour.head())


def get_historical_price_minute(coin, to_curr=CURR, exchange=EXCHANGE, toTs=time.time(), **kwargs):
    """ Get price per min for past 24 hours """
    resp = query_cryptocompare(
        get_url(
            'histominute',
            fsym=coin,
            tsym=to_curr,
            e=exchange,
            toTs=int(toTs),
            **kwargs
        )
    )
    return get_readable_df(resp)

# print(get_historical_price_minute(COIN))


def get_historical_price_minute_by_day(*args, days_ago=0, **kwargs):
    """ Get price per min for 24 hours till days_ago """
    if days_ago > 7:
        logging.error("Can not get information by minute for more than 7 days. Getting information for last possible day.")
        days_ago = 7
    days_ago -= 1  # Subtracting one day as toTs considers ending timestamp
    ts = datetime.datetime.today() - datetime.timedelta(days_ago)
    ts = time.mktime(ts.timetuple())
    return get_historical_price_minute(*args, **kwargs, toTs=int(ts))

# print(get_historical_price_minute_by_day(COIN, days_ago=7))
