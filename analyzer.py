# Created on:   03/03/2018  Aditya Shirode
# Modified on:  03/08/2018  Aditya Shirode


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


import os
import time
import logging
import datetime
import importlib
import dateutil.parser
import ccxt
import numpy as np
import pandas as pd
from collections import defaultdict
import talib
import pyti
from pyti import bollinger_bands
from pyti import money_flow_index
from pyti import stochastic
from pyti import simple_moving_average
from pyti import stochrsi
from pyti import on_balance_volume

LOGGING_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.
                    INFO)
import cryptocompare as ccw
from apscheduler.schedulers.background import BackgroundScheduler

# FIELDS
PRICE = 'PRICE'
HIGH = 'HIGH24HOUR'
LOW = 'LOW24HOUR'
VOLUME = 'VOLUME24HOUR'
CHANGE = 'CHANGE24HOUR'
CHANGE_PERCENT = 'CHANGEPCT24HOUR'
MARKETCAP = 'MKTCAP'
NPERIODS = 100
TIMEFRAME = 'Day'
CURR = 'BTC'
EXCHANGE = 'CCCAGG'
COIN = 'ETH'
COIN_LIST = ['BTC', 'ETH', 'XRP']
EXCHANGES = ['Bittrex', 'Binance', 'Kucoin', 'HuobiPro', 'Cryptopia', 'IDEX']
EXCHANGES = ['Bittrex']
number_of_coins = 0

# For every exchange, fetch it's markets. Then depending on the JSON returned, prepare a list of coins for which historical data has to be downloaded.
bittrex_exchange = ccxt.bittrex()
binance_exchange = ccxt.binance()
kucoin_exchange = ccxt.kucoin()
huobiPro_exchange = ccxt.huobipro()
cryptopia_exchange = ccxt.cryptopia()
list_of_exchanges = [bittrex_exchange, binance_exchange, kucoin_exchange, huobiPro_exchange,
                     cryptopia_exchange]
done = False
i = 0

coins_list = set()
var_quote = ""

# Maps csv (future data objects) to period granularity
# If we store all data together in a single data source, we'll change this to a function which returns corresponding rows
data_csv_period_mapping = {
    "1day": 'all_coins_day_full_1day.csv',
    "1hour": 'all_coins_hour_full_1hour.csv',
    "1min": 'all_coins_min_full_1min.csv',
    "1daycryptopia": 'all_coins_day_full_1day_Cryptopia.csv'
}
frequency_resampling_period_mapping = {
    "day": 'D',
    "hour": 'H',
    "min": 'M'
}
function_period_mapping = {
    '1day': ccw.get_historical_price_day,
    '1hour': ccw.get_historical_price_hour,
    '1min': ccw.get_historical_price_minute,
    '1dayfull': ccw.get_historical_price_day_full,
    '1daycryptopia': ccw.get_historical_price_day
}

indicator_list = ['unix_timestamp', 'BBANDS_BANDWIDTH_PERCENT', 'MONEY_FLOW_INDEX',
                  'STOCH_PERCENT_K_MONEY_FLOW_INDEX', 'STOCH_PERCENT_D_MONEY_FLOW_INDEX', 'RSI', 'RSI_OVER_BOUGHT',
                  'RSI_OVER_SOLD',
                  'STOCHRSI_K', 'STOCHRSI_D', 'STOCH_PERCENT_K', 'STOCH_PERCENT_D', 'STOCH_OVER_BOUGHT',
                  'STOCH_OVER_SOLD', 'SMA_FAST', 'SMA_SLOW', 'SMA_TEST',
                  'MACD', 'MACD_SIGNAL', 'MACD_TEST', 'ON_BALANCE_VOLUME', 'ON_BALANCE_VOLUME_TEST']

# Technical Analysis Settings
EMA_FAST = 10
EMA_SLOW = 20
RSI_PERIOD = 14
RSI_OVER_BOUGHT = 70
RSI_OVER_SOLD = 30
RSI_AVG_PERIOD = 15
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
STOCH_K = 14
STOCH_D = 3
STOCH_OVER_BOUGHT = 70
STOCH_OVER_SOLD = 30

for exchange in list_of_exchanges:
    if exchange.name == 'Cryptopia' or exchange.name == 'Binance' or exchange.name == 'Kucoin' or exchange.name == 'Huobi Pro':
        continue
    markets = exchange.fetchMarkets()
    for row in markets:
        if exchange.name == 'Huobi Pro' or exchange.name == 'Cryptopia':
            if row['base'] not in coins_list:
                coins_list.add(row['base'])
            continue
        if 'active' in row and row['active'] == True:
            if exchange.name == 'Bittrex' or exchange.name == 'Binance':
                var_quote = "quoteId"
            elif exchange.name == 'Kucoin' or exchange.name == 'Huobi Pro':
                var_quote = "quote"
            # print(var_quote)
            if var_quote in row and row[var_quote] == 'BTC':
                if row['base'] not in coins_list:
                    coins_list.add(row['base'])
print(list(coins_list))


def download_new_coins(csv_filename_read, csv_filename_write, timeframe):
    csv_all_coins_full = csv_filename_read
    csv_all_coins_full_new = csv_filename_write
    not_updated = defaultdict(list)
    existing_coin_exchange = []
    # If the csv already exists, find out which coins and exchanges have already been added
    if os.path.isfile(csv_all_coins_full):
        df_csv_all_coins_full = pd.read_csv(csv_all_coins_full, index_col=['coin', 'exchange'])
        # existing_coin_exchange is a list of tuples (coin, exchange)
        existing_coin_exchange = np.unique(df_csv_all_coins_full.index.values)

    for exchange in EXCHANGES:
        for symbol in coins_list:
            # For every symbol-exchange combination, if it is present in CSV,don't download historical Data for it.
            combination_present = False
            for item in existing_coin_exchange:
                if item[0] == symbol and item[1] == exchange:
                    print("Combination Present")
                    combination_present = True
                    break
            if combination_present == True:
                print(symbol, exchange, "This will continue")
                continue
            try:
                # Can't fetch the same symbol in same symbol rate
                print(symbol, exchange, "In Try Block")
                func = function_period_mapping[timeframe]
                to_curr = 'BTC'
                if exchange == 'IDEX':
                    to_curr = 'ETH'
                if symbol is not to_curr:
                    df_coin_all = func(
                        coin=symbol,
                        to_curr=to_curr,
                        timestamp=time.time(),
                        exchange=exchange
                    )

                if df_coin_all.empty:
                    not_updated[exchange].append(symbol)
                else:
                    df_coin_all['exchange'] = exchange
                    df_coin_all['coin'] = symbol
                    df_coin_all = df_coin_all.reset_index().set_index(['coin', 'exchange', 'time'])
                    # If csv does not exist, write, else append
                    if not os.path.isfile(csv_all_coins_full_new):
                        df_coin_all.to_csv(csv_all_coins_full_new, mode='w')
                    else:
                        df_coin_all.to_csv(csv_all_coins_full_new, mode='a', header=False)
                    number_of_coins = number_of_coins + 1

            except Exception as e:
                logging.error(e)
                not_updated[exchange].append(symbol)

    logging.error("Did not update the following. Try again.\n {not_updated}".format(not_updated=not_updated))
    print(number_of_coins)


download_new_coins('all_coins_day_full_1day.csv', 'all_coins_day_full_1day_new_coins.csv', '1dayfull')

download_new_coins('all_coins_hour_full_1hour_.csv', 'all_coins_hour_full_1hour_.csv', '1hour')




def update_indicator(csv_filename, periods, timeframe, datetimeformat_string):
    """ Update the given csv_file with new column values for corr rows """
    df_csv = pd.read_csv(csv_filename, index_col=None, dayfirst=True)

    for indicator in indicator_list:
        if indicator not in df_csv.columns and indicator not in df_csv.index:
            df_csv[indicator] = np.nan
    df_csv.unix_timestamp = df_csv.time.apply(
        lambda t: time.mktime(datetime.datetime.strptime(str(t), datetimeformat_string).timetuple()))
    df_csv = df_csv.set_index(['coin', 'exchange', 'unix_timestamp'])
    data = list(df_csv.index.get_level_values(0).unique())
    i = 0
    j = 0
    for coin_name in data:
        coin_df = df_csv[df_csv.index.get_level_values(0) == coin_name]
        # print(coin_df)
        coin_df = coin_df.reset_index()
        coin_df = coin_df.sort_values(by=['exchange', 'unix_timestamp']).set_index(
            ['coin', 'exchange', 'unix_timestamp'])
        # print(coin_df)
        df_groupby = coin_df.groupby(['exchange'], group_keys=False)
        for key, item in df_groupby:
            req_data = df_groupby.get_group(key)
            req_data2 = req_data.iloc[-periods:]

            start_date = req_data2.index.get_level_values(2)[0]
            end_date = req_data2.index.get_level_values(2)[req_data2.shape[0] - 1]
            req_data2 = req_data[
                (req_data.index.get_level_values(2) >= start_date) & (req_data.index.get_level_values(2) <= end_date)]
            # print(req_data2)
            np_volumeto = np.array(req_data2.volumeto.values, dtype='f8')
            if len(np_volumeto) < 20:
                j = j + 1
                print(coin_name, j, " Not Updated")
                continue
            req_data2['BBANDS_BANDWIDTH_PERCENT'] = pyti.bollinger_bands.percent_b(req_data2.close.values, 20)
            req_data2['MONEY_FLOW_INDEX'] = money_flow_index.money_flow_index(req_data2.close.values,
                                                                              req_data2.high.values,
                                                                              req_data2.low.values, np_volumeto, 14)
            req_data2['STOCH_PERCENT_K_MONEY_FLOW_INDEX'] = pyti.stochastic.percent_k(req_data2.MONEY_FLOW_INDEX.values,
                                                                                      14) * 100
            req_data2['STOCH_PERCENT_D_MONEY_FLOW_INDEX'] = pyti.simple_moving_average.simple_moving_average(
                req_data2.STOCH_PERCENT_K_MONEY_FLOW_INDEX.values, 3)
            req_data2['RSI'] = talib.func.RSI(req_data2.close.values, timeperiod=RSI_PERIOD)
            req_data2['RSI_OVER_BOUGHT'] = np.where(
                (req_data2.RSI >= RSI_OVER_BOUGHT) & (req_data2.RSI <= req_data2.RSI.shift(1)), 1, 0)
            req_data2['RSI_OVER_SOLD'] = np.where(
                (req_data2.RSI <= RSI_OVER_SOLD) & (req_data2.RSI >= req_data2.RSI.shift(1)), 1, 0)
            req_data2['STOCHRSI_K'] = pyti.stochrsi.stochrsi(req_data2.close.values, 14)
            req_data2['STOCHRSI_D'] = pyti.simple_moving_average.simple_moving_average(req_data2.STOCHRSI_K.values, 3)
            req_data2['STOCH_PERCENT_K'] = pyti.stochastic.percent_k(req_data2.high.values, 14) * 100
            req_data2['STOCH_PERCENT_D'] = pyti.simple_moving_average.simple_moving_average(
                req_data2.STOCH_PERCENT_K.values, 3)
            req_data2['STOCH_OVER_BOUGHT'] = np.where((req_data2.STOCH_PERCENT_K >= STOCH_OVER_BOUGHT) & (
                    req_data2.STOCH_PERCENT_K <= req_data2.STOCH_PERCENT_K.shift(1)), 1, 0)
            req_data2['STOCH_OVER_SOLD'] = np.where((req_data2.STOCH_PERCENT_K <= STOCH_OVER_SOLD) & (
                    req_data2.STOCH_PERCENT_K >= req_data2.STOCH_PERCENT_K.shift(1)), 1, 0)
            req_data2['SMA_FAST'] = talib.func.SMA(req_data2.close.values, 7)
            req_data2['SMA_SLOW'] = talib.func.SMA(req_data2.close.values, 21)
            req_data2['SMA_TEST'] = np.where(req_data2.SMA_FAST > req_data2.SMA_SLOW, 1, 0)
            req_data2['ON_BALANCE_VOLUME'] = on_balance_volume.on_balance_volume(req_data2.close.values, np_volumeto)
            req_data2['ON_BALANCE_VOLUME_TEST'] = np.where(req_data2.ON_BALANCE_VOLUME > req_data2.ON_BALANCE_VOLUME.shift(1), 1, 0)
            req_data2['MACD'], req_data2['MACD_SIGNAL'], MACD_HISTOGRAM = talib.func.MACD(req_data2.close.values,
                                                                                  fastperiod=MACD_FAST,
                                                                                  slowperiod=MACD_SLOW,
                                                                                  signalperiod=MACD_SIGNAL)
            req_data2['MACD_TEST'] = np.where(req_data2.MACD > req_data2.MACD_SIGNAL, 1, 0)

            df_csv.update(req_data2)
            i = i + 1
    df_csv.to_csv(csv_filename, date_format="%d-%m-%Y %H:%M:%S")
    print("Done")


update_indicator('all_coins_day_full_1day.csv', 250, '1day', '%d-%m-%Y %H:%M')
print("Done")

update_indicator('all_coins_day_full_3days_.csv', 250, '3day', '%d-%m-%Y %H:%M:%S')
print("Done")

update_indicator('all_coins_day_full_7days_.csv', 250, '7day', '%d-%m-%Y %H:%M:%S')
print("Done")

update_indicator('all_coins_day_full_1day_Cryptopia.csv', 250, '1day', '%d-%m-%Y %H:%M')
print("Done")

update_indicator('all_coins_day_full_3days_Cryptopia.csv', 250, '3day', '%d-%m-%Y %H:%M:%S')
print("Done")

update_indicator('all_coins_day_full_7days_Cryptopia.csv', 250, '7day', '%d-%m-%Y %H:%M:%S')
print("Done")

update_indicator('all_coins_day_full_14days_Cryptopia.csv', 250, '14day', '%d-%m-%Y %H:%M:%S')
print("Done")

update_indicator('all_coins_hour_full_1hour.csv', 250, '1hour',"")
print("Done")

update_indicator('all_coins_hour_full_4hours_.csv', 250, '4hour',"")
print("Done")

update_indicator('all_coins_hour_full_6hours_.csv', 250, '6hour',"")
print("Done")

update_indicator('all_coins_hour_full_12hours_.csv', 250, '12hour',"")
print("Done")

update_indicator('all_coins_min_full_1min.csv', 250, '1min',"")
print("Done")


def resample(csv_filename, period, resampling_multiplier, exchange, datetimeformat_string):
    df_csv = pd.read_csv(csv_filename, dayfirst=True)
    
    if datetimeformat_string is not None or not datetimeformat_string:
        df_csv.time = df_csv.time.apply(lambda t: datetime.datetime.strptime(t, datetimeformat_string))

    df_csv = df_csv.reset_index()

    for indicator in indicator_list:
        if indicator not in df_csv.columns:
            df_csv[indicator] = np.nan
    df_csv = df_csv.set_index(['coin', 'exchange', 'time'])
    data = list(df_csv.index.get_level_values(0).unique())
    i = 0
    all_dataframes = []
    resampling_period = "" + str(resampling_multiplier) + frequency_resampling_period_mapping[period]
    output_csv_filename = "all_coins_" + period + "_full_" + str(
        resampling_multiplier) + period + "s_" + exchange + ".csv"
    for coin_name in data:
        coin_df = df_csv[df_csv.index.get_level_values(0) == coin_name]
        coin_df = coin_df.reset_index()
        coin_df = coin_df.sort_values(by=['exchange', 'time']).set_index(['coin', 'exchange', 'time'])
        # print(coin_df)
        df_groupby = coin_df.groupby(['exchange'], group_keys=False)
        for key, item in df_groupby:
            req_data = df_groupby.get_group(key)
            req_data = req_data.resample(resampling_period, level=2, closed='right', label='right').agg(
                {'open': 'first',
                 'high': 'max',
                 'low': 'min',
                 'close': 'last',
                 'volumeto': 'sum',
                 'volumefrom': 'sum'})

            req_data['coin'] = coin_name
            req_data['exchange'] = key

            # print(req_data)
            req_data = req_data.reset_index()
            req_data['unix_timestamp'] = req_data.time.apply(
                lambda t: time.mktime(datetime.datetime.strptime(str(t), '%Y-%m-%d %H:%M:%S').timetuple()))
            req_data = req_data.set_index(['coin', 'exchange', 'time'])
            i = i + 1
            print(coin_name, i)
            all_dataframes.append(req_data)
    pd.concat(all_dataframes).to_csv(output_csv_filename, date_format="%d-%m-%Y %H:%M:%S")
    print("Done")


resample(data_csv_period_mapping['1day'], 'day', 3, "", '%d-%m-%Y %H:%M')

resample(data_csv_period_mapping['1day'], 'day', 7, "", '%d-%m-%Y %H:%M')

resample(data_csv_period_mapping['1daycryptopia'], 'day', 3, "Cryptopia", '%d-%m-%Y %H:%M')

resample(data_csv_period_mapping['1daycryptopia'], 'day', 7, "Cryptopia", '%d-%m-%Y %H:%M')

resample(data_csv_period_mapping['1daycryptopia'], 'day', 14, "Cryptopia", '%d-%m-%Y %H:%M')

resample(data_csv_period_mapping['1hour'], 'hour', 4, "")

resample(data_csv_period_mapping['1hour'], 'hour', 6, "")

resample(data_csv_period_mapping['1hour'], 'hour', 12, "")


def fetch_data_api(coin=COIN, to_curr=CURR, nperiods=1, period='1day', exchange_name=EXCHANGES[0]):
    """ Fetch data for coin over nperiods
        e.g. Get data for 'BTC' for past 12 hours in hours granularity
    """
    period = period.lower()
    func = function_period_mapping[period]
    if exchange_name == 'IDEX':
        to_curr = 'ETH'
    coin_last_nperiods = func(
        coin=coin,
        to_curr=to_curr,
        limit=nperiods,
        exchange=exchange_name
    )
    if coin_last_nperiods is not None:
        return coin_last_nperiods.iloc[-int(nperiods):]
    else:
        return None


def update_csv_to_latest(period='1day'):
    """ Update the csv for given period upto current time for coin """
    period = period.lower()
    csv_filename = data_csv_period_mapping[period]  # Get corr csv
    # csv_filename = 'Experiment.csv'
    df_coin_period = pd.read_csv(csv_filename)  # , index_col=['coin', 'exchange']
    csv_column_order = df_coin_period.columns.tolist()
    df_coin_period = df_coin_period.set_index(keys=['coin', 'exchange'])
    df_coin_period.time = df_coin_period.time.apply(lambda t: datetime.datetime.strptime(t, '%d-%m-%Y %H:%M'))

    lst_new_data = []
    PRINT_MSG = "{:15} {!s:20} {!s:>20} {:>10}"
    logging.info(PRINT_MSG.format("Exchange", "Last Updated Time", "Elapsed Time", "nPeriodsAgo"))
    coins_in_csv = list(df_coin_period.index.get_level_values(0).unique())
    i = 0
    j = 0
    for coin in coins_in_csv:
        df_coin_period_coin = df_coin_period.loc[coin]
        # Group by exchange, sort on timestamp, and get the last row of that particular coin
        last_update = df_coin_period_coin.groupby('exchange', group_keys=False).apply(
            lambda c: c.sort_values(by='time').tail(1))
        logging.info("-" * 10 + " For coin - {}".format(coin))
        # print(last_update)

        for exchange in last_update.index.values:  # For every coin exchange combination
            last_updated_time = last_update.loc[exchange]['time']  # Get the time of the last row
            try:
                # elapsed_time = datetime.datetime.now() - datetime.datetime.strptime(last_updated_time, '%Y-%m-%d %H:%M:%S')
                # elapsed_time = datetime.datetime.now() - datetime.datetime.strptime(last_updated_time, '%d-%m-%Y %H:%M')
                elapsed_time = datetime.datetime.now() - last_updated_time
            except ValueError as e:
                logging.info("Failed to parse time {} for {}--{}".format(last_updated_time, coin, exchange))
                elapsed_time = datetime.datetime.now() - dateutil.parser.parse(last_updated_time)
            nperiods_ago = elapsed_time / datetime.timedelta(
                days=1 if period == '1day' or period == '1daycryptopia' else 0,
                hours=1 if period == '1hour' else 0,
                minutes=1 if period == '1min' else 0,
                seconds=1)
            nperiods_ago = np.floor(nperiods_ago)

            logging.info(PRINT_MSG.format(exchange, last_updated_time, elapsed_time, nperiods_ago))

            if nperiods_ago > 0:
                """
                logging.info("Updating data for {coin}-{exchange} from {last_updated_time}".format(
                    coin=coin, exchange=exchange, last_updated_time=last_updated_time)
                )"""
                # sys.exit("Testing")
                new_data_coin_period = fetch_data_api(
                    coin=coin,
                    nperiods=nperiods_ago,
                    period=period,
                    exchange_name=exchange
                )
                # print(new_data_coin_period.shape)
                if new_data_coin_period is None:
                    print(coin, exchange, " Info Not available from API", str(j))
                    j = j + 1
                    continue
                new_data_coin_period['coin'] = coin
                new_data_coin_period['exchange'] = exchange
                new_data_coin_period = new_data_coin_period.reset_index()
                new_data_coin_period['unix_timestamp'] = new_data_coin_period.time.apply(
                    lambda t: time.mktime(datetime.datetime.strptime(str(t), '%d-%m-%Y %H:%M:%S').timetuple()))
                i = i + 1
                lst_new_data.append(new_data_coin_period)

    if lst_new_data:
        df_new_data = pd.concat(lst_new_data)
        df_new_data = df_new_data.reset_index()
        curr_columns = df_new_data.columns.tolist()
        column_order = [col for col in csv_column_order if col in curr_columns]
        df_new_data = df_new_data.reindex(columns=column_order)
        df_new_data.to_csv(csv_filename, mode='a', header=False, index=False, date_format="%d-%m-%Y %H:%M:%S")
    print("Done")


update_csv_to_latest('1day')

update_csv_to_latest('1dayCryptopia')

update_csv_to_latest('1hour')

update_csv_to_latest('1min')
