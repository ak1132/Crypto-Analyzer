from lomond import WebSocket
import pandas as pd
import math

df_csv_1day = pd.read_csv(
    'BTC_Bitfinex_day_full_1day.csv', index_col=None, dayfirst=True)
df_csv_1day['timeframe'] = "1 day"

df_csv_3days = pd.read_csv(
    'BTC_Bitfinex_day_full_3days.csv', index_col=None, dayfirst=True)
df_csv_3days['timeframe'] = "3 days"

df_csv_7days = pd.read_csv(
    'BTC_Bitfinex_day_full_7days.csv', index_col=None, dayfirst=True)
df_csv_7days['timeframe'] = "7 days"

df_csv_1hour = pd.read_csv(
    'BTC_Bitfinex_hour_full_1hour.csv', index_col=None, dayfirst=True)
df_csv_1hour['timeframe'] = "1 hour"

df_csv_4hours = pd.read_csv(
    'BTC_Bitfinex_hour_full_4hours.csv', index_col=None, dayfirst=True)
df_csv_4hours['timeframe'] = "4 hours"

df_csv_6hours = pd.read_csv(
    'BTC_Bitfinex_hour_full_6hours.csv', index_col=None, dayfirst=True)
df_csv_6hours['timeframe'] = "6 hours"

df_csv_12hours = pd.read_csv(
    'BTC_Bitfinex_hour_full_12hours.csv', index_col=None, dayfirst=True)
df_csv_12hours['timeframe'] = "12 hours"

df_csv_14days = pd.read_csv(
    'BTC_Bitfinex_day_full_14days.csv', index_col=None, dayfirst=True)


def check_BTC_bollinger_band_Support_Resistance(json_input,
                                                datetimeformat_string):
    BTC_Dataframe_array = [
        df_csv_1day, df_csv_3days, df_csv_7days, df_csv_1hour, df_csv_4hours,
        df_csv_6hours, df_csv_12hours
    ]
    string_to_print1 = ""
    string_to_print2 = ""
    percent_above = 0.0
    percent_below = 0.0
    for dataframe in BTC_Dataframe_array:
        df_csv = dataframe
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
                row = req_data.tail(1)
                if 'UPPER_BOLLINGER_BAND_VALUE' not in row:
                    continue
                BTC_UPPER_BAND, BTC_MIDDLE_BAND, BTC_LOWER_BAND = float(
                    row['UPPER_BOLLINGER_BAND_VALUE']), float(
                        row['MIDDLE_BOLLINGER_BAND_VALUE']), float(
                            row['LOWER_BOLLINGER_BAND_VALUE'])
                output_from_json = json_input
                if 'price' not in output_from_json:
                    continue
                if math.isnan(BTC_UPPER_BAND):
                    continue
                current_price = float(output_from_json['price'])

                print(BTC_UPPER_BAND, BTC_MIDDLE_BAND, BTC_LOWER_BAND,
                      row['timeframe'])
                difference = current_price - BTC_UPPER_BAND
                if difference > 0:
                    percent_above = (difference / BTC_UPPER_BAND) * 100
                    string_to_print1 = str(current_price) + " is " + str(
                        percent_above
                    ) + " % above case 1 " + dataframe['timeframe'] + " resistance  " + str(
                        BTC_UPPER_BAND)
                elif difference < 0:
                    percent_below = (-difference / BTC_UPPER_BAND) * 100
                    string_to_print1 = str(current_price) + " is " + str(
                        percent_below
                    ) + " % below case 2 " + dataframe['timeframe'] + " resistance  " + str(
                        BTC_UPPER_BAND)
                else:
                    string_to_print1 = str(
                        current_price
                    ) + " is at case 3 " + dataframe['timeframe'] + " resistance  " + str(
                        BTC_UPPER_BAND)
                difference = current_price - BTC_LOWER_BAND
                if difference > 0:
                    percent_above = (difference / BTC_LOWER_BAND) * 100
                    string_to_print2 = str(current_price) + " is " + str(
                        percent_above
                    ) + " % above case 4 " + dataframe['timeframe'] + " support  " + str(
                        BTC_LOWER_BAND)
                elif difference < 0:
                    percent_below = (-difference / BTC_LOWER_BAND) * 100
                    string_to_print2 = str(current_price) + " is " + str(
                        percent_below
                    ) + " % below case 5 " + dataframe['timeframe'] + " support  " + str(
                        BTC_LOWER_BAND)
                else:
                    string_to_print2 = str(
                        current_price
                    ) + " is at case 6 " + dataframe['timeframe'] + " support  " + str(
                        BTC_LOWER_BAND)
    if percent_above <= 3.0:
        print(string_to_print1)
    if percent_below <= 3.0:
        print(string_to_print2)


if __name__ == '__main__':
    websocket = WebSocket('wss://ws-feed.gdax.com')

    for event in websocket:
        if event.name == "ready":
            websocket.send_json(
                type='subscribe', product_ids=['BTC-USD'], channels=['ticker'])
        elif event.name == "text":
            check_BTC_bollinger_band_Support_Resistance(
                event.json, '%d-%m-%Y %H:%M')
