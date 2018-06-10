import logging
import requests
import pandas as pd

client_id = "283_3vv0ugxcf8kkggk0goowww8sckwck4kgsw8okg8cw0wg44o40g"
client_secret = "5vq8b6o6qxs08goc80oocccokscgo0ow0o4swk8wkg8o0s4kc"
grant_type = "client_credentials"
coins_list_url = "https://api.coinmarketcal.com/v1/coins?"
access_token_url = "https://api.coinmarketcal.com/oauth/v2/token?"
events_list_url = "https://api.coinmarketcal.com/v1/events?"


def query_coinmarketcal_get_access_token(grant, client, secret):
    """ Query CoinMarketCal API """
    url = access_token_url + "grant_type=" + grant + "&client_id=" + client + "&client_secret=" + secret
    try:
        response = requests.get(url).json()
    except Exception as e:
        logging.error("Failure while querying {query}. \n{err}".format(query=url, err=e))
        return None

    if not response:  # or 'Response' not in response.keys():
        return response
    return response


access_token = query_coinmarketcal_get_access_token(grant_type, client_id, client_secret)['access_token']


def query_coinmarketcal_get_coins_list(access_token):
    """ Query CoinMarketCal API to get Coins List"""
    coins_url = coins_list_url + "access_token=" + access_token
    try:
        response = requests.get(coins_url).json()
    except Exception as e:
        logging.error("Failure while querying {query}. \n{err}".format(query=coins_url, err=e))
        return None

    if not response:  # or 'Response' not in response.keys():
        return response
    return response


df = pd.DataFrame(query_coinmarketcal_get_coins_list(
    query_coinmarketcal_get_access_token(grant_type, client_id, client_secret)['access_token']))
df.to_csv("coins.csv")
print("Done")


def query_coinmarketcal_get_events_list(access_token):
    """ Query CoinMarketCal API to get Events List"""
    loop = True
    i = 1
    final_response = []
    while (loop):
        events_url = events_list_url + "access_token=" + access_token + "&page=" + str(i)
        try:
            response = requests.get(events_url).json()
            if 'code' in response and response.code == 404:
                loop = False
                break
            i = i + 1
            for _ in response:
                final_response.append(response)
        except Exception as e:
            logging.error("Failure while querying {query}. \n{err}".format(query=events_url, err=e))
            loop = False
    return final_response

if __name__=='__main__':
    json_response = query_coinmarketcal_get_events_list(
    query_coinmarketcal_get_access_token(grant_type, client_id, client_secret)['access_token'])
    df = pd.DataFrame(json_response)
    df.to_csv("Events.csv")
