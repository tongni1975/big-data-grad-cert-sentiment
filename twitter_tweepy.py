import os
from unicodedata import name
import tweepy as tw
import pandas as pd
import requests
import schedule
import io
import sys
import time
import pickle

consumer_key= 'uNd6MVlFa8oY3xTJVBYln58TT'
consumer_secret= '432nI4pvTs7Zdky2r5XFz1dPmK6gY4ICvr6MoI98oHAbq2G25P'
access_token= '39466086-TL4hXrfnpfnuWNtxMgE98fAyYgZ1sVCz2ptLa9lG4'
access_token_secret= 'KAzO6WDvRgD3HMN0Tp6YmXI8koxeBxDmh2xxbWjMLwABH'
bearer_token='AAAAAAAAAAAAAAAAAAAAAC4IaAEAAAAAKzkzNlGe4day%2BuuLEDkEnsrk8dk%3D7VWohGI1nWWfSnWvYPlLknlGPRLAdIrUhzUPrCPUohKtCkNDjK'
client_id='cnhHaEZMelozN1RfV3Q0SnkydWs6MTpjaQ'
client_secret='cpNEoHPOep9z_7d4UexItiORHttigWXdK5v7Rwp7RpKlrdAUj9'
# auth = tw.OAuthHandler(consumer_key, consumer_secret)
# auth.set_access_token(access_token, access_token_secret)
# api = tw.API(auth, wait_on_rate_limit=True)


# public_tweets = api.home_timeline()
# for tweet in public_tweets:
#     print(tweet.text)

# v2 api client
twc = tw.Client(bearer_token=bearer_token, 
                       consumer_key=consumer_key, 
                       consumer_secret=consumer_secret, 
                       access_token=access_token, 
                       access_token_secret=access_token_secret)

# search tweets
# Define the search term and the date_since date as variables
search_words = "bitcoin"
date_since = "2018-11-16"
twitter_fields = ['id', 'text', 'author_id', 'created_at', 'geo']

# Collect tweets
def get_tweets():
   tweets = twc.search_recent_tweets(query=search_words, tweet_fields = twitter_fields, max_results=100)
   twits = [[tweet.id, tweet.text, tweet.author_id, tweet.created_at, tweet.geo] for tweet in tweets.data]

   #todo clean up all RTs

   # output to file name
   name = time.strftime('tweets_%d%m%Y_%H_%M.csv')
   pd.DataFrame(data=twits, columns=twitter_fields).to_csv('input/{}'.format(name))
   #pd.DataFrame(data=twits, columns=twitter_fields).to_csv('tweets_{}.csv', time.strftime("%Y%m%d-%H%M%S"))



schedule.every(1).minutes.do(get_tweets)
while True:
   #schedule.run_pending()
   schedule.run_all(delay_seconds=10)

   time.sleep(1)