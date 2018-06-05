import configparser

cfile = open('config.ini','w')
cParser = configparser.ConfigParser()

cParser.add_section('coinmarketcal_api')
cParser.set('coinmarketcal_api','client_id',"283_3vv0ugxcf8kkggk0goowww8sckwck4kgsw8okg8cw0wg44o40g")
cParser.set('coinmarketcal_api','client_secret', "5vq8b6o6qxs08goc80oocccokscgo0ow0o4swk8wkg8o0s4kc")
cParser.set('coinmarketcal_api','grant_type', "client_credentials")
cParser.set('coinmarketcal_api','coins_list_url', "https://api.coinmarketcal.com/v1/coins?")
cParser.set('coinmarketcal_api','access_token_url', "https://api.coinmarketcal.com/oauth/v2/token?")
cParser.set('coinmarketcal_api','events_list_url', "https://api.coinmarketcal.com/v1/events?")

cParser.add_section('coinmarketcap')
cParser.set('coinmarketcap','listing','https://api.coinmarketcap.com/v2/listings/')
cParser.set('coinmarketcap','ticker','https://api.coinmarketcap.com/v2/ticker/?convert=BTC&start=')


cParser.write(cfile)

cfile.close()

