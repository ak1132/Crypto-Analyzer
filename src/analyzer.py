# coding: utf-8

# Created on:   03/03/2018  Aditya Shirode
# Modified on:  07/03/2018  Amogh Kulkarni

# TO DO:
# - Make generic functions for tasks for modularity
# - CRON jobs for all timeframes
# - One function to update all csvs
# - Plug n play for indicators
# - Add limit to queries
# - Include Active/Inactive column in CSV for active/inactive coins
# - Update_CSV_to_Latest should contain active coins from Exchanges and From CSV. Check if a (coin,exchange) tuple is active(Check if it is present on exchange using ccxt library) . If it is active   get latest data for it if already present in CSV,if not in CSV get all data. If coin is not active on exchange , we will put a Active/Inactive status in CSV accordingly. All functions will have   to be modified to run code only for active coin-exchange combinations.
# - Date Format will be '%d-%m-%Y %H:%M:%S' . This is giving me a lot of problems especially while reading data. When I don't put :%S it tells me dataframe has second and sometimes when :%S is         there, it tells me no second value in dataframe.
# - Analytics Value Accuracy. Some parameter in Jupyter.
# - Have to fetch Coins based on Parameters. Example - Fetch all active coins-exchange combinations where RSI>0 and RSI<=30. Fetch all active coins-exchange combinations where closing price is         between LOWERBAND and MIDDLEBAND. Get me intersection(common coins) of these 2 list. Now the coin from the intersection list which probably has the lowest volume can increase in price faster       then the others(Little increase in Volume will result in Big increase in Price)
# - For each active coin-exchange combination I want to check the change in Value of different Technical Indicators of 2 consecutive periods in time. Example - I want to know if for a particular       coin RSI=a on period x and RSI>a on period x+1. I want to know whenever MACD and MACD_SIGNAL cross each other(On period x MACD=a and MACD_SIGNAL=b where a<=b and on period x+1 MACD=a and           MACD_SIGNAL=b where a>b. MACD_HISTOGRAM same like RSI want to know when it is 'a' on period x and 'a++' on period x+1).
# - Convert 1D timeframe to 3D/1Week/etc. Convert 1H timeframe to 4H/6H/etc.
# - Batch Processing of downloading new coins data

# Imports
import os
import time
import talib
import logging
import datetime
import dateutil.parser
import ccxt
import numpy as np
import pandas as pd
from collections import defaultdict
import pyti
from apscheduler.schedulers.background import BackgroundScheduler
from pyti import bollinger_bands
from pyti import money_flow_index
from pyti import stochastic
from pyti import simple_moving_average
from pyti import stochrsi
from pyti import on_balance_volume
import cryptocompare as ccw
import configparser
from database import DbClient

configParser = configparser.ConfigParser()
configParser.read(os.curdir + '\\resources\config.ini')
relativePath = os.path.abspath(os.path.join(os.curdir, "..")) + '\\'

dbClient = DbClient()

LOGGING_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=LOGGING_FORMAT)

datetimeStringformat_to_csv = "%d-%m-%Y %H:%M"
CURR = 'BTC'
COIN = 'ETH'
COIN_LIST = ['BTC', 'ETH', 'XRP']
EXCHANGES = ['Bittrex', 'Binance', 'Kucoin', 'HuobiPro', 'Cryptopia', 'IDEX']

# Maps csv (future data objects) to period granularity
# If we store all data together in a single data source,
# we'll change this to a function which returns corresponding rows
data_csv_period_mapping = {
    "1day": 'all_coins_day_full_1day.csv',
    "1hour": 'all_coins_hour_full_1hour.csv',
    "1min": 'all_coins_min_full_1min.csv',
    "1daycryptopia": 'all_coins_day_full_1day_Cryptopia.csv',
    '1daybtcbitfinex': 'BTC_Bitfinex_day_full_1day.csv',
    '1hourbtcbitfinex': 'BTC_Bitfinex_hour_full_1hour.csv'
}

frequency_resampling_period_mapping = {"day": 'D', "hour": 'H', "min": 'M'}

function_period_mapping = {
    '1day': ccw.get_historical_price_day,
    '1hour': ccw.get_historical_price_hour,
    '1min': ccw.get_historical_price_minute,
    '1dayfull': ccw.get_historical_price_day_full,
    '1daycryptopia': ccw.get_historical_price_day,
    '1daybtcbitfinex': ccw.get_historical_price_day,
    '1hourbtcbitfinex': ccw.get_historical_price_hour
}

indicator_list = [
    'unix_timestamp', 'BBANDS_BANDWIDTH_PERCENT', 'MONEY_FLOW_INDEX',
    'STOCH_PERCENT_K_MONEY_FLOW_INDEX', 'STOCH_PERCENT_D_MONEY_FLOW_INDEX',
    'RSI', 'RSI_OVER_BOUGHT', 'RSI_OVER_SOLD', 'STOCHRSI_K', 'STOCHRSI_D',
    'STOCH_PERCENT_K', 'STOCH_PERCENT_D', 'STOCH_OVER_BOUGHT',
    'STOCH_OVER_SOLD', 'SMA_FAST', 'SMA_SLOW', 'SMA_TEST', 'MACD',
    'MACD_SIGNAL', 'MACD_TEST', 'ON_BALANCE_VOLUME', 'ON_BALANCE_VOLUME_TEST'
]


# For every exchange, fetch it's markets. Then depending on the JSON returned,
# prepare a list of coins for which historical data has to be downloaded.

def get_exchange_list():
    bittrex_exchange = ccxt.bittrex()
    binance_exchange = ccxt.binance()
    kucoin_exchange = ccxt.kucoin()
    huobiPro_exchange = ccxt.huobipro()
    cryptopia_exchange = ccxt.cryptopia()
    bitmex_exchange = ccxt.bitmex()
    list_of_exchanges = [
        bittrex_exchange, binance_exchange, kucoin_exchange, huobiPro_exchange,
        cryptopia_exchange
    ]
    return list_of_exchanges


def setupExchanges(list_of_exchanges):
    var_quote = ""
    coin_exchange_combination = {}
    for exchange in list_of_exchanges:
        coins_list = set()
        markets = exchange.fetchMarkets()
        for row in markets:
            if exchange.name == 'Huobi Pro' or exchange.name == 'Cryptopia':
                if row['base'] not in coins_list:
                    coins_list.add(row['base'])
            if 'active' in row and row['active'] == True:
                if exchange.name == 'Bittrex' or exchange.name == 'Binance':
                    var_quote = "quoteId"
                elif exchange.name == 'Kucoin' or exchange.name == 'Huobi Pro':
                    var_quote = "quote"
                if var_quote in row and row[var_quote] == 'BTC':
                    if row['base'] not in coins_list:
                        coins_list.add(row['base'])
        coin_exchange_combination[exchange.name] = coins_list
    return coin_exchange_combination


def update_and_delete_coin_exchange_combination(csv_filename_read,
                                                csv_filename_write, timeframe,
                                                coin_exchange_combination):
    # For every exchange, download the new coins and delete coins which are delisted from that exchange
    csv_all_coins_full = relativePath + "data\\" + csv_filename_read
    csv_all_coins_full_new = relativePath + "data\\" + csv_filename_write
    not_updated = defaultdict(list)
    # If the csv already exists, find out which coins and exchanges have already been added
    if os.path.isfile(csv_all_coins_full):
        df_csv_all_coins_full = pd.read_csv(
            csv_all_coins_full, index_col=['exchange', 'coin'])
        # existing_coin_exchange is a list of tuples (coin, exchange)
        existing_coin_exchange = np.unique(df_csv_all_coins_full.index.values)
        coin_exchange_combination_in_excel = {}

        for a, b in existing_coin_exchange:
            coin_exchange_combination_in_excel.setdefault(a, []).append(b)

    number_of_coins = 0
    coin_exchange_combination_to_delete = {}

    for exchange in coin_exchange_combination:
        if exchange not in coin_exchange_combination_in_excel:
            continue
        coins_list_from_exchange = coin_exchange_combination[exchange]
        coins_list_from_excel = coin_exchange_combination_in_excel[exchange]
        coins_to_download = list(set(coins_list_from_exchange) - set(coins_list_from_excel))
        coins_to_delete = list(set(coins_list_from_excel) - set(coins_list_from_exchange))
        coin_exchange_combination_to_delete[exchange] = coins_to_delete

        for symbol in coins_to_download:
            # For every symbol-exchange combination, if it is present in CSV,don't download historical Data for it.
            try:
                # Can't fetch the same symbol in same symbol rate
                mapped_period_function = function_period_mapping[timeframe]
                to_curr = 'BTC'

                if symbol == "BTC":
                    to_curr = "USD"

                if exchange == 'IDEX':
                    to_curr = 'ETH'

                if symbol is not to_curr:
                    df_coin_all = mapped_period_function(coin=symbol, to_curr=to_curr, timestamp=time.time(),
                                                         exchange=exchange)

                if df_coin_all.empty:
                    not_updated[exchange].append(symbol)
                else:
                    df_coin_all['exchange'] = exchange
                    df_coin_all['coin'] = symbol
                    df_coin_all['time'] = pd.to_datetime(
                        df_coin_all.unix_timestamp, unit="s", utc=True)
                    df_coin_all = df_coin_all.reset_index().set_index(
                        ['coin', 'exchange', 'unix_timestamp'])

                    # If csv does not exist, write, else append
                    if not os.path.isfile(csv_all_coins_full_new):
                        df_coin_all.to_csv(csv_all_coins_full_new, mode='w')
                    else:
                        df_coin_all.to_csv(csv_all_coins_full_new, mode='a', header=False)
                    number_of_coins = number_of_coins + 1

            except Exception as e:
                logging.error(e)
                not_updated[exchange].append(symbol)
                logging.error(
                    "Did not update the following. Try again.\n {not_updated}".format(
                        not_updated=not_updated))

    delete_coins_from_csv(coin_exchange_combination_to_delete,
                          df_csv_all_coins_full.reset_index(),
                          csv_filename_read)


def delete_coins_from_csv(coin_exchange_combination_to_delete,
                          df_csv_all_coins_full, csv_filename_read):
    for exchange in coin_exchange_combination_to_delete:
        coins_list_to_delete = coin_exchange_combination_to_delete[exchange]
        for symbol in coins_list_to_delete:
            df_csv_all_coins_full = df_csv_all_coins_full[~(
                    (df_csv_all_coins_full['coin'] == symbol) &
                    (df_csv_all_coins_full['exchange'] == exchange))]
    df_csv_all_coins_full.set_index(
        ['coin', 'exchange', 'unix_timestamp']).to_csv(csv_filename_read)

    dbClient.save_to_db(pd.read_csv(csv_filename_read), 'all coins')


def update_indicator(csv_filename, periods, timeframe, datetimeformat_string):
    """ Update the given csv_file with new column values for corr rows """
    df_csv = pd.read_csv(csv_filename, index_col=None, dayfirst=True)
    df_csv.drop_duplicates(
        subset=['coin', 'exchange', 'unix_timestamp'], inplace=True)

    for indicator in indicator_list:
        if indicator not in df_csv.columns and indicator not in df_csv.index:
            df_csv[indicator] = np.nan

    df_csv = df_csv.set_index(['coin', 'exchange', 'unix_timestamp'])
    data = list(df_csv.index.get_level_values(0).unique())
    i = 0
    j = 0

    for coin_name in data:
        coin_df = df_csv[df_csv.index.get_level_values(0) == coin_name]
        coin_df = coin_df.reset_index()
        coin_df = coin_df.sort_values(by=['exchange', 'unix_timestamp']).set_index(
            ['coin', 'exchange', 'unix_timestamp'])

        df_by_exchange = coin_df.groupby(['exchange'], group_keys=False)

        for key, item in df_by_exchange:
            req_data = df_by_exchange.get_group(key)
            req_data2 = req_data.iloc[-periods:]
            start_date = req_data2.index.get_level_values(2)[0]
            end_date = req_data2.index.get_level_values(2)[req_data2.shape[0] - 1]
            req_data2 = req_data[(req_data.index.get_level_values(2) >= start_date)
                                 & (req_data.index.get_level_values(2) <= end_date)]
            np_volumeto = np.array(req_data2.volumeto.values, dtype='f8')

            if len(np_volumeto) < 20:
                j = j + 1
                print(coin_name, j, " Not Updated")
                continue

            req_data2[
                'BBANDS_BANDWIDTH_PERCENT'] = pyti.bollinger_bands.percent_b(
                req_data2.close.values, 20)
            req_data2['MONEY_FLOW_INDEX'] = money_flow_index.money_flow_index(
                req_data2.close.values, req_data2.high.values,
                req_data2.low.values, np_volumeto, 14)
            req_data2[
                'STOCH_PERCENT_K_MONEY_FLOW_INDEX'] = pyti.stochastic.percent_k(
                req_data2.MONEY_FLOW_INDEX.values, 14) * 100
            req_data2[
                'STOCH_PERCENT_D_MONEY_FLOW_INDEX'] = pyti.simple_moving_average.simple_moving_average(
                req_data2.STOCH_PERCENT_K_MONEY_FLOW_INDEX.values, 3)
            req_data2['RSI'] = talib.func.RSI(
                req_data2.close.values, timeperiod=configParser.getint('technical_settings', 'rsi_period'))
            req_data2['RSI_OVER_BOUGHT'] = np.where(
                (req_data2.RSI >= configParser.getint('technical_settings', 'rsi_over_bought')) &
                (req_data2.RSI <= req_data2.RSI.shift(1)), 1, 0)
            req_data2['RSI_OVER_SOLD'] = np.where(
                (req_data2.RSI <= configParser.getint('technical_settings', 'rsi_over_sold')) &
                (req_data2.RSI >= req_data2.RSI.shift(1)), 1, 0)
            req_data2['STOCHRSI_K'] = pyti.stochrsi.stochrsi(
                req_data2.close.values, 14)
            req_data2[
                'STOCHRSI_D'] = pyti.simple_moving_average.simple_moving_average(
                req_data2.STOCHRSI_K.values, 3)
            req_data2['STOCH_PERCENT_K'] = pyti.stochastic.percent_k(
                req_data2.high.values, 14) * 100
            req_data2[
                'STOCH_PERCENT_D'] = pyti.simple_moving_average.simple_moving_average(
                req_data2.STOCH_PERCENT_K.values, 3)
            req_data2['STOCH_OVER_BOUGHT'] = np.where(
                (req_data2.STOCH_PERCENT_K >= configParser.getint('technical_settings', 'stoch_over_bought')) &
                (req_data2.STOCH_PERCENT_K <=
                 req_data2.STOCH_PERCENT_K.shift(1)), 1, 0)
            req_data2['STOCH_OVER_SOLD'] = np.where(
                (req_data2.STOCH_PERCENT_K <= configParser.getint('technical_settings', 'stoch_over_sold')) &
                (req_data2.STOCH_PERCENT_K >=
                 req_data2.STOCH_PERCENT_K.shift(1)), 1, 0)
            req_data2['SMA_FAST'] = talib.func.SMA(req_data2.close.values, 7)
            req_data2['SMA_SLOW'] = talib.func.SMA(req_data2.close.values, 21)
            req_data2['SMA_TEST'] = np.where(
                req_data2.SMA_FAST > req_data2.SMA_SLOW, 1, 0)
            req_data2[
                'ON_BALANCE_VOLUME'] = on_balance_volume.on_balance_volume(
                req_data2.close.values, np_volumeto)
            req_data2['ON_BALANCE_VOLUME_TEST'] = np.where(
                req_data2.ON_BALANCE_VOLUME >
                req_data2.ON_BALANCE_VOLUME.shift(1), 1, 0)
            req_data2['MACD'], req_data2[
                'MACD_SIGNAL'], MACD_HISTOGRAM = talib.func.MACD(
                req_data2.close.values,
                fastperiod=configParser.getint('technical_settings', 'macd_fast'),
                slowperiod=configParser.getint('technical_settings', 'macd_slow'),
                signalperiod=configParser.getint('technical_settings', 'macd_signal'))
            req_data2['MACD_TEST'] = np.where(
                req_data2.MACD > req_data2.MACD_SIGNAL, 1, 0)

            df_csv.update(req_data2)
            i = i + 1
    df_csv.to_csv(csv_filename, date_format=datetimeStringformat_to_csv)
    df_csv = pd.read_csv(csv_filename)
    dbClient.save_to_db(df_csv, 'technical data')


def resample(csv_filename, period, resampling_multiplier, exchange,
             datetimeformat_string, output_file_name):
    df_csv = pd.read_csv(csv_filename, dayfirst=True)
    df_csv.unix_timestamp = pd.to_datetime(
        df_csv.unix_timestamp, unit="s", utc=True)
    df_csv = df_csv.reset_index()

    for indicator in indicator_list:
        if indicator not in df_csv.columns:
            df_csv[indicator] = np.nan

    df_csv = df_csv.set_index(['coin', 'exchange', 'unix_timestamp'])
    data = list(df_csv.index.get_level_values(0).unique())
    i = 0
    all_dataframes = []
    resampling_period = "" + str(resampling_multiplier) + \
                        frequency_resampling_period_mapping[period]
    output_csv_filename = output_file_name

    for coin_name in data:
        coin_df = df_csv[df_csv.index.get_level_values(0) == coin_name]
        coin_df = coin_df.reset_index()
        coin_df = coin_df.sort_values(
            by=['exchange', 'unix_timestamp']).set_index(
            ['coin', 'exchange', 'unix_timestamp'])
        df_groupby = coin_df.groupby(['exchange'], group_keys=False)
        for key, item in df_groupby:
            req_data = df_groupby.get_group(key)
            req_data = req_data.resample(
                resampling_period, level=2, closed='right',
                label='right').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volumeto': 'sum',
                'volumefrom': 'sum'
            })

            req_data['coin'] = coin_name
            req_data['exchange'] = key
            req_data = req_data.reset_index()
            req_data['unix_timestamp'] = req_data.unix_timestamp.apply(lambda t: time.mktime(
                datetime.datetime.strptime(str(t), '%Y-%m-%d %H:%M:%S').timetuple()))
            req_data['time'] = pd.to_datetime(
                req_data.unix_timestamp, unit="s", utc=True)

            req_data = req_data.set_index(
                ['coin', 'exchange', 'unix_timestamp'])
            i = i + 1
            print(coin_name, i)
            all_dataframes.append(req_data)
    dataframe_answer = pd.concat(all_dataframes)
    dataframe_answer.to_csv(
        output_csv_filename, date_format=datetimeStringformat_to_csv)


def fetch_data_api(coin=COIN,
                   to_curr=CURR,
                   nperiods=1,
                   period='1day',
                   exchange_name=EXCHANGES[0]):
    """ Fetch data for coin over nperiods
        e.g. Get data for 'BTC' for past 12 hours in hours granularity
    """
    period = period.lower()
    func = function_period_mapping[period]
    if exchange_name == 'IDEX':
        to_curr = 'ETH'
    if coin == 'BTC':
        to_curr = 'USD'
    coin_last_nperiods = func(
        coin=coin, to_curr=to_curr, limit=nperiods, exchange=exchange_name)
    if coin_last_nperiods is not None:
        return coin_last_nperiods.iloc[-int(nperiods):]
    else:
        return None


def delete_latest_period_data(csv_filename, datetimeformat_string):
    df_csv = pd.read_csv(csv_filename, index_col=None, dayfirst=True)
    df_csv = df_csv.set_index(['coin', 'exchange', 'unix_timestamp'])
    data = list(df_csv.index.get_level_values(0).unique())
    i = 0
    j = 0
    final_dataframe = []
    for coin_name in data:
        coin_df = df_csv[df_csv.index.get_level_values(0) == coin_name]

        coin_df = coin_df.reset_index()
        coin_df = coin_df.sort_values(
            by=['exchange', 'unix_timestamp']).set_index(
            ['coin', 'exchange', 'unix_timestamp'])

        df_groupby = coin_df.groupby(['exchange'], group_keys=False)
        for key, item in df_groupby:
            req_data = df_groupby.get_group(key)
            req_data = req_data[:-1]

            final_dataframe.append(req_data)
    answer = pd.concat(final_dataframe).reset_index()

    return answer


def update_csv_to_latest(period='1day',
                         datetimeformat_string='%d-%m-%Y %H:%M',
                         final_csv_column_order=[]):
    """ Update the csv for given period upto current time for coin """
    period = period.lower()
    csv_filename = data_csv_period_mapping[period]  # Get corr csv
    df_coin_period = delete_latest_period_data(csv_filename,
                                               datetimeStringformat_to_csv)

    # Updated Dataframe
    csv_column_order = df_coin_period.columns.tolist()
    df_coin_period = df_coin_period.set_index(keys=['coin', 'exchange'])

    lst_new_data = []
    PRINT_MSG = "{:15} {!s:20} {!s:>20} {:>10}"
    logging.info(
        PRINT_MSG.format("Exchange", "Last Updated Time", "Elapsed Time",
                         "nPeriodsAgo"))
    coins_in_csv = list(df_coin_period.index.get_level_values(0).unique())
    i = 0
    j = 0
    for coin in coins_in_csv:
        df_coin_period_coin = df_coin_period.loc[coin]
        # Group by exchange, sort on timestamp, and get the last row of that particular coin
        last_update = df_coin_period_coin.groupby(
            'exchange', group_keys=False).apply(
            lambda c: c.sort_values(by='unix_timestamp').tail(1))
        logging.info("-" * 10 + " For coin - {}".format(coin))

        for exchange in last_update.index.values:  # For every coin exchange combination
            # Get the time of the last row
            last_updated_time = int(
                last_update.loc[exchange]['unix_timestamp'])
            try:
                elapsed_time = int(time.time()) - last_updated_time
            except ValueError as e:
                logging.info(
                    "Failed to parse time {} for {}--{}".format(
                        last_updated_time, coin, exchange), e)
                elapsed_time = datetime.datetime.now() - dateutil.parser.parse(
                    last_updated_time)
            nperiods_ago = 0

            if period == '1day' or period == '1daycryptopia' or period == '1daybtcbitfinex':
                nperiods_ago = elapsed_time / (60 * 60 * 24)
            elif period == '1hour' or period == '1hourbtcbitfinex':
                nperiods_ago = elapsed_time / (60 * 60)

            nperiods_ago = np.floor(nperiods_ago)
            logging.info(
                PRINT_MSG.format(exchange, last_updated_time, elapsed_time,
                                 nperiods_ago))

            if nperiods_ago > 0:

                logging.info(
                    "Updating data for {coin}-{exchange} from {last_updated_time}".
                        format(
                        coin=coin,
                        exchange=exchange,
                        last_updated_time=last_updated_time))

                new_data_coin_period = fetch_data_api(
                    coin=coin,
                    nperiods=nperiods_ago,
                    period=period,
                    exchange_name=exchange)

                if new_data_coin_period is None:
                    print(coin, exchange, " Info Not available from API",
                          str(j))
                    j = j + 1
                    continue
                new_data_coin_period['coin'] = coin
                new_data_coin_period['exchange'] = exchange
                new_data_coin_period = new_data_coin_period.reset_index()
                new_data_coin_period['time'] = pd.to_datetime(
                    new_data_coin_period.unix_timestamp, unit="s", utc=True)
                i = i + 1

                lst_new_data.append(new_data_coin_period)

    if lst_new_data:
        df_new_data = pd.concat(lst_new_data)
        df_new_data = df_new_data.reset_index()
        curr_columns = df_new_data.columns.tolist()

        df_coin_period = df_coin_period.reset_index()
        csv_column_order = df_coin_period.columns.tolist()
        column_order = [col for col in csv_column_order if col in curr_columns]

        df_new_data = df_new_data.reindex(columns=column_order)
        df_coin_period = df_coin_period.append(df_new_data)

        df_coin_period = df_coin_period[final_csv_column_order]
        df_coin_period.drop_duplicates(
            subset=['coin', 'exchange', 'unix_timestamp'], inplace=True)
        df_coin_period.set_index(['coin', 'exchange',
                                  'unix_timestamp']).to_csv(
            csv_filename,
            date_format=datetimeStringformat_to_csv)


indicator_list_btc = ['unix_timestamp', 'UPPER_BOLLINGER_BAND_VALUE', 'MIDDLE_BOLLINGER_BAND_VALUE',
                      'LOWER_BOLLINGER_BAND_VALUE']


def update_indicator_BTC(csv_filename, periods, timeframe,
                         datetimeformat_string):
    """ Update the given csv_file with new column values for corr rows """
    df_csv = pd.read_csv(csv_filename, index_col=None, dayfirst=True)

    for indicator in indicator_list_btc:
        if indicator not in df_csv.columns and indicator not in df_csv.index:
            df_csv[indicator] = np.nan

    df_csv = df_csv.set_index(['coin', 'exchange', 'unix_timestamp'])
    data = list(df_csv.index.get_level_values(0).unique())
    i = 0
    j = 0
    for coin_name in data:
        coin_df = df_csv[df_csv.index.get_level_values(0) == coin_name]

        coin_df = coin_df.reset_index()
        coin_df = coin_df.sort_values(
            by=['exchange', 'unix_timestamp']).set_index(
            ['coin', 'exchange', 'unix_timestamp'])

        df_groupby = coin_df.groupby(['exchange'], group_keys=False)
        for key, item in df_groupby:
            req_data = df_groupby.get_group(key)
            req_data2 = req_data.iloc[-periods:]

            start_date = req_data2.index.get_level_values(2)[0]
            end_date = req_data2.index.get_level_values(2)[req_data2.shape[0] -
                                                           1]
            req_data2 = req_data[
                (req_data.index.get_level_values(2) >= start_date)
                & (req_data.index.get_level_values(2) <= end_date)]

            np_volumeto = np.array(req_data2.volumeto.values, dtype='f8')
            if len(np_volumeto) < 20:
                j = j + 1
                print(coin_name, j, " Not Updated")
                continue
            req_data2[
                'UPPER_BOLLINGER_BAND_VALUE'] = pyti.bollinger_bands.upper_bollinger_band(
                req_data2.close.values, 20)
            req_data2[
                'MIDDLE_BOLLINGER_BAND_VALUE'] = pyti.bollinger_bands.middle_bollinger_band(
                req_data2.close.values, 20)
            req_data2[
                'LOWER_BOLLINGER_BAND_VALUE'] = pyti.bollinger_bands.lower_bollinger_band(
                req_data2.close.values, 20)
            df_csv.update(req_data2)
            i = i + 1
            print(coin_name, i)

    df_csv.to_csv(csv_filename, date_format=datetimeformat_string)


def changeCSVDateTimeFormat(csv_filename):
    df_csv = pd.read_csv(
        csv_filename,
        index_col=None,
        dayfirst=True,
        infer_datetime_format=True)
    df_csv['time'] = pd.to_datetime(df_csv.unix_timestamp, unit="s", utc=True)
    df_csv.set_index(['coin', 'exchange', 'unix_timestamp']).to_csv(
        csv_filename, date_format=datetimeStringformat_to_csv)


if __name__ == '__main__':
    coins_list_from_exchange = setupExchanges(get_exchange_list())

    update_and_delete_coin_exchange_combination(
        'all_coins_day_full_1day.csv', 'all_coins_day_full_1day_new_coins.csv',
        '1dayfull', coins_list_from_exchange)

    coins_list = [""]

    update_and_delete_coin_exchange_combination(
        'all_coins_hour_full_1hour_.csv', 'all_coins_hour_full_1hour_.csv',
        '1hour', coins_list)

    update_indicator('all_coins_day_full_1day.csv', 250, '1day',
                     datetimeStringformat_to_csv)
    '''
    update_indicator('all_coins_day_full_3days.csv', 250, '3day',
                     datetimeStringformat_to_csv)

    update_indicator('all_coins_day_full_7days.csv', 250, '7day',
                     datetimeStringformat_to_csv)

    update_indicator('all_coins_day_full_14days.csv', 250, '14day',
                     datetimeStringformat_to_csv)

    update_indicator('all_coins_day_full_1day_Cryptopia.csv', 250, '1day',
                     datetimeStringformat_to_csv)

    update_indicator('all_coins_day_full_3days_Cryptopia.csv', 250, '3day',
                     datetimeStringformat_to_csv)

    update_indicator('all_coins_day_full_7days_Cryptopia.csv', 250, '7day',
                     datetimeStringformat_to_csv)

    update_indicator('all_coins_day_full_14days_Cryptopia.csv', 250, '14day',
                     datetimeStringformat_to_csv)

    update_indicator('all_coins_min_full_1min.csv', 250, '1min', "")

    resample(data_csv_period_mapping['1day'], 'day', 3, "",
             datetimeStringformat_to_csv, 'all_coins_day_full_3days.csv')

    resample(data_csv_period_mapping['1day'], 'day', 7, "",
             datetimeStringformat_to_csv, 'all_coins_day_full_7days.csv')

    resample(data_csv_period_mapping['1day'], 'day', 14, "",
             datetimeStringformat_to_csv, 'all_coins_day_full_14days.csv')

    resample(data_csv_period_mapping['1daycryptopia'], 'day', 3, "Cryptopia",
             datetimeStringformat_to_csv,
             'all_coins_day_full_3days_Cryptopia.csv')

    resample(data_csv_period_mapping['1daycryptopia'], 'day', 7, "Cryptopia",
             datetimeStringformat_to_csv,
             'all_coins_day_full_7days_Cryptopia.csv')

    resample(data_csv_period_mapping['1daycryptopia'], 'day', 14, "Cryptopia",
             datetimeStringformat_to_csv,
             'all_coins_day_full_14days_Cryptopia.csv')

    resample(data_csv_period_mapping['1hourbtcbitfinex'], 'hour', 4,
             "Bitfinex", datetimeStringformat_to_csv,
             'BTC_Bitfinex_hour_full_4hours.csv')

    resample(data_csv_period_mapping['1hourbtcbitfinex'], 'hour', 6,
             "Bitfinex", datetimeStringformat_to_csv,
             'BTC_Bitfinex_hour_full_6hours.csv')

    resample(data_csv_period_mapping['1hourbtcbitfinex'], 'hour', 12,
             "Bitfinex", datetimeStringformat_to_csv,
             'BTC_Bitfinex_hour_full_12hours.csv')

    resample(data_csv_period_mapping['1daybtcbitfinex'], 'day', 3, "Bitfinex",
             datetimeStringformat_to_csv, 'BTC_Bitfinex_day_full_3days.csv')

    resample(data_csv_period_mapping['1daybtcbitfinex'], 'day', 7, "Bitfinex",
             datetimeStringformat_to_csv, 'BTC_Bitfinex_day_full_7days.csv')

    resample(data_csv_period_mapping['1daybtcbitfinex'], 'day', 14, "Bitfinex",
             datetimeStringformat_to_csv, 'BTC_Bitfinex_day_full_14days.csv')

    columns_order = [
        'coin', 'exchange', 'unix_timestamp', 'time', 'open', 'high', 'low',
        'close', 'volumefrom', 'volumeto', 'BBANDS_BANDWIDTH_PERCENT', 'MACD',
        'MACD_SIGNAL', 'MACD_TEST', 'MONEY_FLOW_INDEX', 'ON_BALANCE_VOLUME',
        'ON_BALANCE_VOLUME_TEST', 'RSI', 'RSI_OVER_BOUGHT', 'RSI_OVER_SOLD',
        'SMA_FAST', 'SMA_SLOW', 'SMA_TEST', 'STOCHRSI_D', 'STOCHRSI_K',
        'STOCH_OVER_BOUGHT', 'STOCH_OVER_SOLD', 'STOCH_PERCENT_D',
        'STOCH_PERCENT_D_MONEY_FLOW_INDEX', 'STOCH_PERCENT_K',
        'STOCH_PERCENT_K_MONEY_FLOW_INDEX'
    ]

    update_csv_to_latest('1day', datetimeStringformat_to_csv, columns_order)

    update_csv_to_latest('1dayCryptopia', datetimeStringformat_to_csv,
                         columns_order)

    columns_order = [
        'coin', 'exchange', 'unix_timestamp', 'time', 'open', 'high', 'low',
        'close', 'volumefrom', 'volumeto', 'UPPER_BOLLINGER_BAND_VALUE',
        'MIDDLE_BOLLINGER_BAND_VALUE', 'LOWER_BOLLINGER_BAND_VALUE'
    ]
    update_csv_to_latest('1daybtcbitfinex', datetimeStringformat_to_csv,
                         columns_order)

    columns_order = [
        'coin', 'exchange', 'unix_timestamp', 'time', 'open', 'high', 'low',
        'close', 'volumefrom', 'volumeto', 'UPPER_BOLLINGER_BAND_VALUE',
        'MIDDLE_BOLLINGER_BAND_VALUE', 'LOWER_BOLLINGER_BAND_VALUE'
    ]

    update_csv_to_latest('1hourbtcbitfinex', datetimeStringformat_to_csv,
                         columns_order)

    indicator_list_btc = [
        'unix_timestamp', 'UPPER_BOLLINGER_BAND_VALUE',
        'MIDDLE_BOLLINGER_BAND_VALUE', 'LOWER_BOLLINGER_BAND_VALUE'
    ]

    update_indicator_BTC('BTC_Bitfinex_day_full_1day.csv', 250, '1day',
                         datetimeStringformat_to_csv)

    update_indicator_BTC('BTC_Bitfinex_day_full_3days.csv', 250, '3day',
                         datetimeStringformat_to_csv)

    update_indicator_BTC('BTC_Bitfinex_day_full_7days.csv', 250, '7day',
                         datetimeStringformat_to_csv)

    update_indicator_BTC('BTC_Bitfinex_day_full_14days.csv', 250, '14day',
                         datetimeStringformat_to_csv)

    update_indicator_BTC('BTC_Bitfinex_hour_full_1hour.csv', 250, '1hour',
                         datetimeStringformat_to_csv)

    update_indicator_BTC('BTC_Bitfinex_hour_full_4hours.csv', 250, '4hour',
                         datetimeStringformat_to_csv)

    update_indicator_BTC('BTC_Bitfinex_hour_full_6hours.csv', 250, '6hour',
                         datetimeStringformat_to_csv)

    update_indicator_BTC('BTC_Bitfinex_hour_full_12hours.csv', 250, '12hour',
                         datetimeStringformat_to_csv)

    changeCSVDateTimeFormat('all_coins_day_full_1day.csv')

    changeCSVDateTimeFormat('all_coins_day_full_1day_Cryptopia.csv')

    changeCSVDateTimeFormat('BTC_Bitfinex_day_full_1day.csv')

    changeCSVDateTimeFormat('BTC_Bitfinex_hour_full_1hour.csv')
'''
    columns_order = [
        'coin', 'exchange', 'unix_timestamp', 'time', 'open', 'high', 'low',
        'close', 'volumefrom', 'volumeto', 'UPPER_BOLLINGER_BAND_VALUE',
        'MIDDLE_BOLLINGER_BAND_VALUE', 'LOWER_BOLLINGER_BAND_VALUE'
    ]
    # Scheduler code
    scheduler = BackgroundScheduler()
    scheduler.start()

    scheduler.add_job(
        update_csv_to_latest,
        "cron",
        ['1daybtcbitfinex', datetimeStringformat_to_csv, columns_order],
        minute='*/1')

    scheduler.shutdown(wait=False)
