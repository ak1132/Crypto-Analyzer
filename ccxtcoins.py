import time
import ccxt
import logging
import datetime
import pandas as pd
import os

columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']


def foo(somelist, columns2):
    dct = {}
    i = 0
    for column in columns2:
        if i == 0:
            somelist[i] = convert_timestamp(somelist[i])
        if i == 5:
            somelist[i] = somelist[i] * somelist[i - 1]
        dct[column] = somelist[i]
        i = i + 1
    return dct


def convert_timestamp(timestamp):
    """ Convert timestamp into readable datetime """
    timestamp = timestamp / 1000.0
    try:
        return datetime.datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logging.debug(e)
        return None


def get_readable_df(response):
    """ Extract data from given response and return a dataframe """
    try:
        df_data = pd.DataFrame(response)
    except AttributeError as e:
        logging.debug(e)
        return None
    return df_data


def get_df_from_lists(arr):
    dct = []
    for item in arr:
        dct.append(foo(item, columns))
    return get_readable_df(dct)


bittrex_exchange = ccxt.bittrex()
binance_exchange = ccxt.binance()
kucoin_exchange = ccxt.kucoin()

list_of_exchanges = [bittrex_exchange,
                     binance_exchange]

coins_list = set()
done = False
df = pd.DataFrame(columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol', 'Exchange'])
for exchange in list_of_exchanges:
    if exchange.name == 'Bittrex':
        divideby = 50
    elif exchange.name == 'Binance':
        divideby = 16.6666
    if exchange.has['fetchOHLCV']:
        market = exchange.fetchMarkets()
        j = 0
        for row in market:
            if row['active'] == True and 'quoteId' in row and row['quoteId'] == 'BTC':
                if row['symbol'] not in coins_list:
                    time.sleep(exchange.rateLimit / divideby)
                    arr = exchange.fetch_ohlcv(symbol=row['symbol'], timeframe='1d', limit=200)
                    j = j + 1
                    df_coin = get_df_from_lists(arr)
                    df_coin['Symbol'] = row['symbol']
                    df_coin['Exchange'] = exchange.name
                    if not os.path.isfile('ccxt1Day.csv'):
                        df_coin.to_csv('ccxt1Day.csv', mode='w')
                    else:
                        df_coin.to_csv('ccxt1Day.csv', mode='a', header=False)
                    df = df.append(df_coin)
                    coins_list.add(row['symbol'])
