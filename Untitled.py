import pandas as pd
import ccxt
import cryptopia
import os
import time
import talib
import logging
import requests
import datetime
import importlib
import dateutil.parser
import numpy as np
from importlib import reload
from collections import defaultdict
import sys
import inspect
import pyti

indicator_list = ['BBANDS_BANDWIDTH_PERCENT','MONEY_FLOW_INDEX',
                   'STOCH_PERCENT_K_MONEY_FLOW_INDEX','STOCH_PERCENT_D_MONEY_FLOW_INDEX','RSI','RSI_OVER_BOUGHT','RSI_OVER_SOLD',
                   'STOCHRSI_K','STOCHRSI_D','STOCH_PERCENT_K','STOCH_PERCENT_D','STOCH_OVER_BOUGHT','STOCH_OVER_SOLD','SMA_FAST','SMA_SLOW','SMA_TEST',
                  'MACD','MACD_SIGNAL','MACD_TEST','ON_BALANCE_VOLUME','ON_BALANCE_VOLUME_TEST']   


# In[31]:


df_csv = pd.read_csv('all_coins_day_full_1day_Cryptopia.csv')
df_csv = df_csv.set_index(['coin', 'exchange','unix_timestamp'])
df2 = pd.read_csv('order_book_csvCryptopia.csv')
data = list(df_csv.index.get_level_values(0).unique())
i=0
j=0
margin = 0.2
rows=[]
for coin_name in data:
    coin_df = df_csv[df_csv.index.get_level_values(0)==coin_name] 
    #print(coin_df)
    coin_df = coin_df.reset_index()
    coin_df = coin_df.sort_values(by=['exchange','unix_timestamp']).set_index(['coin', 'exchange','unix_timestamp'])
    #print(coin_df)
    df_groupby = coin_df.groupby(['exchange'], group_keys=False)
    for key, item in df_groupby:
        req_data = df_groupby.get_group(key)
        row = req_data.tail(1)
        coin_close_price = float(row['close'])
        #print(coin_name,key,coin_close_price)
        _plust = (margin*coin_close_price)+coin_close_price
        _minust = coin_close_price-(margin*coin_close_price)
        rep = df2.loc[(df2['coin'] == coin_name) & (df2['exchange'] == key)]
        total = rep[(rep['bid_price'] >= _minust) & (rep['bid_price'] <= _plust)]    
        row['bid_amount_btc_total'] = total['bid_amount_btc'].sum()
        row['ask_amount_btc_total'] = total['ask_amount_btc'].sum()
        row = row.reset_index()
        columns_wanted = ['coin','exchange','unix_timestamp','close','bid_amount_btc_total','ask_amount_btc_total']
        row = row[columns_wanted]
        rows.append(row)
pd.concat(rows).to_csv('Order_Book_Analysis.csv')
print("Done")

