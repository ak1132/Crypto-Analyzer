import requests
import pandas as pd

r = requests.get('https://api.coinmarketcap.com/v2/listings/')
length = len(r.json()['data'])
print(length)
res = []
j=0
coins_list = []
for i in range(1,length+1,100):
    r = requests.get('https://api.coinmarketcap.com/v2/ticker/?convert=BTC&start='+str(i))
    for attribute,value in r.json().items():
        #print(attribute,value)
        
        for id,val in value.items():
            coin = {}
            if type(val) is dict:
                for key in val:
                    if key == "quotes": 
                        market_cap_USD = val[key]['USD']['market_cap']
                        coin['market_cap_USD'] = market_cap_USD
                        market_cap_BTC = val[key]['BTC']['market_cap']
                        coin['market_cap_BTC'] = market_cap_BTC
                        price_BTC = val[key]['BTC']['price']
                        coin['price_BTC'] = price_BTC
                        volume_24hr_BTC = val[key]['BTC']['volume_24h']
                        coin['volume_24hour_BTC'] = volume_24hr_BTC
                        volume_24hr_USD = val[key]['USD']['volume_24h']
                        coin['volume_24hour_USD'] = volume_24hr_USD
                        continue
                    coin[key] = val[key]
            j =j+1
            coins_list.append(coin)
dataframe = pd.DataFrame(coins_list)
dataframe.to_csv('coinmarketcap.csv')


