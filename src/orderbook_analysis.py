import ccxt
import logging
import pandas as pd

EXCHANGES = ['Bittrex', 'Binance', 'Kucoin', 'HuobiPro', 'Cryptopia', 'IDEX']
bittrex_exchange = ccxt.bittrex()
binance_exchange = ccxt.binance()
kucoin_exchange = ccxt.kucoin()
huobiPro_exchange = ccxt.huobipro()
cryptopia_exchange = ccxt.cryptopia()
bitfinex_exchange = ccxt.bitfinex()
list_of_exchanges = {
    "Bittrex": bittrex_exchange,
    "Binance": binance_exchange,
    "Kucoin": kucoin_exchange,
    "HuobiPro": huobiPro_exchange,
    "Cryptopia": cryptopia_exchange,
    "Bitfinex": bitfinex_exchange
}
ABSOLUTE_PATH = "../data/"


def get_coin_exchange_past_trades(csv_filename, exchange):
    fields = ['coin', 'exchange', 'time']
    path = ABSOLUTE_PATH + csv_filename
    df_csv = pd.read_csv(
        path, index_col=None, skipinitialspace=True, usecols=fields)
    df_csv = df_csv.set_index(['coin', 'exchange', 'time'])
    data = list(df_csv.index.get_level_values(0).unique())
    i = 0
    j = 0
    past_trades_array = []
    start = 1
    for coin_name in data:
        coin_df = df_csv[df_csv.index.get_level_values(0) == coin_name]
        coin_df = coin_df.reset_index()
        coin_df = coin_df.sort_values(by=['exchange', 'time']).set_index(
            ['coin', 'exchange', 'time'])
        df_groupby = coin_df.groupby(['exchange'], group_keys=False)

        for key, item in df_groupby:
            to_coin = 'BTC'
            if key == 'IDEX':
                to_coin = 'ETH'
                continue
            symbol = coin_name + "/" + to_coin
            print(coin_name, key, start)
            past_trades = list_of_exchanges[key].fetchTrades(symbol)
            if len(past_trades) == 0:
                print("Past Trades Not Retrieved", coin_name, key)
                j = j + 1
            for past_trade in past_trades:
                del past_trade['info']
                past_trade['id'] = start
                past_trade['exchange'] = key
                start = start + 1
                df_past_trade = pd.DataFrame(past_trade, index=['id'])
                past_trades_array.append(df_past_trade)
    pd.concat(past_trades_array).to_csv('Past_Trades' + exchange + '.csv')


def get_coin_exchange_order_book(csv_filename, periods, timeframe,
                                 datetimeformat_string, exchange):
    """ Update the given csv_file with new column values for corr rows """
    fields = ['coin', 'exchange', 'time']
    path = ABSOLUTE_PATH + csv_filename
    df_csv = pd.read_csv(
        path, index_col=None, skipinitialspace=True, usecols=fields)
    df_csv = df_csv.set_index(['coin', 'exchange', 'time'])
    data = list(df_csv.index.get_level_values(0).unique())
    i = 0
    j = 0
    order_book_array = []
    start = 1
    for coin_name in data:
        coin_df = df_csv[df_csv.index.get_level_values(0) == coin_name]
        coin_df = coin_df.reset_index()
        coin_df = coin_df.sort_values(by=['exchange', 'time']).set_index(
            ['coin', 'exchange', 'time'])
        df_groupby = coin_df.groupby(['exchange'], group_keys=False)

        for key, item in df_groupby:
            to_coin = 'BTC'

            if key == 'IDEX':
                to_coin = 'ETH'
                continue

            if coin_name == "BTC":
                to_coin = "USD"
            symbol = coin_name + "/" + to_coin

            try:
                order_book_json = list_of_exchanges[key].fetch_order_book(
                    symbol)
            except Exception as e:
                logging.log(logging.DEBUG, e)
                continue

            if bool(order_book_json) == False:
                print("Order book Not Retrieved", coin_name, key)
                j = j + 1
            create_bid_json = []
            order_book_bids = order_book_json['bids']

            if len(order_book_bids) == 0:
                continue
            for order_book_bid in order_book_bids:
                bid_json = {}
                bid_json['bid_price'] = order_book_bid[0]
                bid_json['bid_amount'] = order_book_bid[1]
                bid_json[
                    'bid_amount_btc'] = order_book_bid[0] * order_book_bid[1]
                bid_json['timestamp'] = order_book_json['timestamp']
                bid_json['coin'] = coin_name
                bid_json['exchange'] = key
                create_bid_json.append(bid_json)

            order_book_asks = order_book_json['asks']
            create_ask_json = []
            if len(order_book_asks) == 0:
                continue
            for order_book_ask in order_book_asks:
                ask_json = {}
                ask_json['ask_price'] = order_book_ask[0]
                ask_json['ask_amount'] = order_book_ask[1]
                ask_json[
                    'ask_amount_btc'] = order_book_ask[0] * order_book_ask[1]
                ask_json['timestamp'] = order_book_json['timestamp']
                ask_json['coin'] = coin_name
                ask_json['exchange'] = key
                create_ask_json.append(ask_json)

            df1 = pd.DataFrame(create_bid_json)
            df2 = pd.DataFrame(create_ask_json)
            size = df1.shape[0]
            df1['id'] = pd.Series(
                range(start, size + start, 1), index=df1.index)
            size = df2.shape[0]
            df2['id'] = pd.Series(
                range(start, size + start, 1), index=df2.index)
            how1 = ""
            if df1.shape[0] > df2.shape[0]:
                how1 = "left"
                start = start + df1.shape[0]
            else:
                how1 = "right"
                start = start + df2.shape[0]
            result = pd.merge(df1, df2, on='id', how=how1)
            if df1.shape[0] > df2.shape[0]:
                result = result.drop(
                    ['coin_y', 'exchange_y', 'timestamp_y'], axis=1)
                result = result.rename(
                    columns={
                        'coin_x': "coin",
                        'exchange_x': "exchange",
                        'timestamp_x': "timestamp"
                    })
            else:
                result = result.drop(
                    ['coin_x', 'exchange_x', 'timestamp_x'], axis=1)
                result = result.rename(
                    columns={
                        'coin_y': "coin",
                        'exchange_y': "exchange",
                        'timestamp_y': "timestamp"
                    })
            i = i + 1
            order_book_array.append(result)
    pd.concat(order_book_array).set_index('id').to_csv('order_book_csv_' +
                                                       exchange + '.csv')
    logging.log(logging.INFO, csv_filename + " generated successfully")


def order_book_analysis(csv_filename, order_book_filename, exchange):
    path = ABSOLUTE_PATH + csv_filename
    df_csv = pd.read_csv(path)
    df_csv = df_csv.set_index(['coin', 'exchange', 'unix_timestamp'])
    df2 = pd.read_csv(order_book_filename)
    data = list(df_csv.index.get_level_values(0).unique())
    i = 0
    j = 0
    margin = 0.2
    rows = []
    for coin_name in data:
        coin_df = df_csv[df_csv.index.get_level_values(0) == coin_name]
        #print(coin_df)
        coin_df = coin_df.reset_index()
        coin_df = coin_df.sort_values(
            by=['exchange', 'unix_timestamp']).set_index(
                ['coin', 'exchange', 'unix_timestamp'])
        #print(coin_df)
        df_groupby = coin_df.groupby(['exchange'], group_keys=False)
        for key, item in df_groupby:
            req_data = df_groupby.get_group(key)
            row = req_data.tail(1)
            coin_close_price = float(row['close'])
            print(coin_name, key)
            _plust = (margin * coin_close_price) + coin_close_price
            _minust = coin_close_price - (margin * coin_close_price)
            rep = df2.loc[(df2['coin'] == coin_name)
                          & (df2['exchange'] == key)]
            total = rep[(rep['bid_price'] >= _minust)
                        & (rep['bid_price'] <= _plust)]
            row['bid_amount_btc_total'] = total['bid_amount_btc'].sum()
            row['ask_amount_btc_total'] = total['ask_amount_btc'].sum()
            row['BID_ASK_VOLUME_DIFFERENCE'] = row['bid_amount_btc_total'] - row['ask_amount_btc_total']
            row = row.reset_index()
            columns_wanted = [
                'coin', 'exchange', 'unix_timestamp', 'close',
                'bid_amount_btc_total', 'ask_amount_btc_total',
                'BID_ASK_VOLUME_DIFFERENCE'
            ]
            row = row[columns_wanted]
            rows.append(row)
    pd.concat(rows).to_csv(ABSOLUTE_PATH + 'Order_Book_Analysis_' + exchange +
                           '.csv')


def order_book_and_price_bollinger_band_analysis(
        order_book_analysis_file, all_coins_day_full, bollinger_band_value,
        number_of_orders):
    """For every exchange this function will give me all coins which are very down and have a big buy wall. Those coins are most likely to increase"""
    df_csv = pd.read_csv(ABSOLUTE_PATH + all_coins_day_full)
    df_csv = df_csv.set_index(['coin', 'exchange', 'unix_timestamp'])
    data = list(df_csv.index.get_level_values(0).unique())
    i = 0
    j = 0
    margin = 0.2
    rows = []
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
            rows.append(row)
    df_latest_coin_data = pd.concat(rows)
    df_latest_coin_data = df_latest_coin_data.reset_index().sort_values(
        by=['exchange', 'BBANDS_BANDWIDTH_PERCENT']).set_index(
            ['coin', 'exchange', 'unix_timestamp'])
    df_order_book = pd.read_csv(order_book_analysis_file)
    df_order_book = df_order_book.reset_index().sort_values(
        by=['exchange', 'BID_ASK_VOLUME_DIFFERENCE'],
        ascending=[True,
                   False]).set_index(['coin', 'exchange', 'unix_timestamp'])
    df_groupby_order_book = df_order_book.groupby(
        ['exchange'], group_keys=False)
    df_groupby_latest_coin_data = df_latest_coin_data.groupby(
        ['exchange'], group_keys=False)
    coins = []
    for key, item in df_groupby_order_book:
        req_data = df_groupby_order_book.get_group(key)
        get_coins = req_data.head(number_of_orders).reset_index()
        first_10_rows_order_book = get_coins['coin']
        req_data2 = df_groupby_latest_coin_data.get_group(key)
        first_n_rows_coin_data = req_data2[(
            req_data2['BBANDS_BANDWIDTH_PERCENT'] <
            bollinger_band_value)].reset_index()
        get_coins2 = first_n_rows_coin_data['coin']
        coins.append(
            list(set(first_10_rows_order_book).intersection(set(get_coins2))))


if __name__ == '__main__':
    '''get_coin_exchange_past_trades('all_coins_day_full_1day.csv',
                                  'Bittrex-Binance-Kucoin')'''
    get_coin_exchange_order_book('all_coins_day_full_1day.csv', 250, '1day',
                                 '%d-%m-%Y %H:%M', 'Bittrex-Binance-Kucoin')
    '''get_coin_exchange_order_book('all_coins_day_full_1day_Cryptopia.csv', 250,
                                 '1day', '%d-%m-%Y %H:%M', 'Cryptopia')
    get_coin_exchange_order_book('BTC_Bitfinex_day_full_1day.csv', 250, '1day',
                                 '%d-%m-%Y %H:%M', 'Bitfinex')

    order_book_analysis('all_coins_day_full_1day_Cryptopia.csv',
                        'order_book_csv_Cryptopia.csv', 'Cryptopia')
    order_book_analysis('all_coins_day_full_1day.csv',
                        'order_book_csv_Bittrex-Binance-Kucoin.csv',
                        'Bittrex-Binance-Kucoin')
    order_book_analysis('BTC_Bitfinex_day_full_1day.csv',
                        'order_book_csv_Bitfinex.csv', 'Bitfinex')

    order_book_and_price_bollinger_band_analysis(
        'Order_Book_Analysis_Bittrex-Binance-Kucoin.csv',
        'all_coins_day_full_3days.csv', 30, 15)
    order_book_and_price_bollinger_band_analysis(
        'Order_Book_Analysis_Bittrex-Binance-Kucoin.csv',
        'all_coins_day_full_7days.csv', 30, 15)
    order_book_and_price_bollinger_band_analysis(
        'Order_Book_Analysis_Cryptopia.csv',
        'all_coins_day_full_14days_Cryptopia.csv', 25, 15)
    order_book_and_price_bollinger_band_analysis(
        'Order_Book_Analysis_Cryptopia.csv',
        'all_coins_day_full_7days_Cryptopia.csv', 25, 15)'''
