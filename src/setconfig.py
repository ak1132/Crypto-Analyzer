import configparser
import os

cfile = open(os.path.join(os.curdir + '\\resources', 'config.ini'), 'w')
cParser = configparser.ConfigParser()

cParser.add_section('coinmarketcal_api')
cParser.set('coinmarketcal_api', 'client_id', "283_3vv0ugxcf8kkggk0goowww8sckwck4kgsw8okg8cw0wg44o40g")
cParser.set('coinmarketcal_api', 'client_secret', "5vq8b6o6qxs08goc80oocccokscgo0ow0o4swk8wkg8o0s4kc")
cParser.set('coinmarketcal_api', 'grant_type', "client_credentials")
cParser.set('coinmarketcal_api', 'coins_list_url', "https://api.coinmarketcal.com/v1/coins?")
cParser.set('coinmarketcal_api', 'access_token_url', "https://api.coinmarketcal.com/oauth/v2/token?")
cParser.set('coinmarketcal_api', 'events_list_url', "https://api.coinmarketcal.com/v1/events?")

cParser.add_section('coinmarketcap')
cParser.set('coinmarketcap', 'listing', 'https://api.coinmarketcap.com/v2/listings/')
cParser.set('coinmarketcap', 'ticker', 'https://api.coinmarketcap.com/v2/ticker/?convert=BTC&start=')

cParser.add_section('database')
cParser.set('database', 'name', 'crypto_analyzer')
cParser.set('database', 'host', 'localhost')
cParser.set('database', 'port', '8086')
cParser.set('database', 'user', '')
cParser.set('database', 'password', '')

cParser.add_section('technical_settings')
cParser.set('technical_settings', 'EMA_FAST', '10')
cParser.set('technical_settings', 'EMA_SLOW', '20')
cParser.set('technical_settings', 'RSI_PERIOD', '14')
cParser.set('technical_settings', 'RSI_OVER_BOUGHT', '70')
cParser.set('technical_settings', 'RSI_OVER_SOLD', '30')
cParser.set('technical_settings', 'RSI_AVG_PERIOD', '15')
cParser.set('technical_settings', 'MACD_FAST', '12')
cParser.set('technical_settings', 'MACD_SLOW', '26')
cParser.set('technical_settings', 'MACD_SIGNAL', '9')
cParser.set('technical_settings', 'STOCH_K', '14')
cParser.set('technical_settings', 'STOCH_D', '3')
cParser.set('technical_settings', 'STOCH_OVER_BOUGHT', '70')
cParser.set('technical_settings', 'STOCH_OVER_SOLD', '30')

cParser.write(cfile)

cfile.close()
