import tweepy as tw
import pandas as pd
import schedule
import time
from datetime import datetime, timedelta


consumer_key = 'uNd6MVlFa8oY3xTJVBYln58TT'
consumer_secret = '432nI4pvTs7Zdky2r5XFz1dPmK6gY4ICvr6MoI98oHAbq2G25P'
access_token = '39466086-TL4hXrfnpfnuWNtxMgE98fAyYgZ1sVCz2ptLa9lG4'
access_token_secret = 'KAzO6WDvRgD3HMN0Tp6YmXI8koxeBxDmh2xxbWjMLwABH'
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAC4IaAEAAAAAKzkzNlGe4day%2BuuLEDkEnsrk8dk%3D7VWohGI1nWWfSnWvYPlLknlGPRLAdIrUhzUPrCPUohKtCkNDjK'
client_id = 'cnhHaEZMelozN1RfV3Q0SnkydWs6MTpjaQ'
client_secret = 'cpNEoHPOep9z_7d4UexItiORHttigWXdK5v7Rwp7RpKlrdAUj9'

# 2nd set (Yawen account)
# consumer_key = '7nuIWjVrUaGnCqukqKT0sSIbX'
# consumer_secret = 'Ms6FVMvHQkAhdda7S6YCYbN3W07YbfX5BEwQhsFTAxtt4PvRfL'
# access_token = '1503349395513016321-f5kEm6q7GEeOhqB1D0yb3MJwdZX0KU'
# access_token_secret = 'diLlEu0tgBDKEpwkervI1vKWtUeCe0VewHrgVPbPjcp9t'
# bearer_token = 'AAAAAAAAAAAAAAAAAAAAADouawEAAAAArdZ3rEKaTVuRB2nS6bcV5%2BV9HMM%3DtGLNcGAe1xkOPIPDNyhO0nJuirDUXX1ZENdTQNs2lzfdYoDJiy'

# v2 api client
twc = tw.Client(bearer_token=bearer_token,
                consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                access_token=access_token,
                access_token_secret=access_token_secret,
                wait_on_rate_limit=True)

# search tweets
# Define the search term and the date_since date as variables
search_words = "#bitcoin lang:en"
#date_begin = datetime.today() - timedelta(days=7)
date_begin = datetime.fromisoformat("2022-03-23 17:49")
date_end = datetime.fromisoformat("2022-03-26 22:00")
twitter_fields = ['id', 'text', 'author_id', 'created_at', 'geo']

# Collect tweets


def get_tweets():
    tweets = twc.search_recent_tweets(
        query=search_words, tweet_fields=twitter_fields, max_results=100)
    twits = [[tweet.id, tweet.text, tweet.author_id,
              tweet.created_at, tweet.geo] for tweet in tweets.data]

    # output to file name
    name = time.strftime('tweets_%d%m%Y_%H_%M.csv')
    pd.DataFrame(data=twits, columns=twitter_fields).to_csv(
        'input/{}'.format(name))
    #pd.DataFrame(data=twits, columns=twitter_fields).to_csv('tweets_{}.csv', time.strftime("%Y%m%d-%H%M%S"))


def get_tweets_by_date(s_dt):
    tweets = twc.search_recent_tweets(
        query=search_words, tweet_fields=twitter_fields,
        start_time=s_dt, end_time=s_dt + timedelta(minutes=1),  max_results=50)

    twits = [[tweet.id, tweet.text, tweet.author_id,
              tweet.created_at, tweet.geo] for tweet in tweets.data]

    # output to file name
    name = s_dt.strftime('tweets_%d%m%Y_%H_%M.csv')
    pd.DataFrame(data=twits, columns=twitter_fields).to_csv(
        'input/past_7/{}'.format(name))
    #pd.DataFrame(data=twits, columns=twitter_fields).to_csv('tweets_{}.csv', time.strftime("%Y%m%d-%H%M%S"))

# with monthly quota of 500,000, for every min we can pull about 50 tweets

# loop query past 7 days tweets @ 1 min interval


# disable this once data is collected
# while date_begin < date_end:
#     get_tweets_by_date(date_begin)
#     date_begin = date_begin + timedelta(minutes=1)


schedule.every(10).minutes.do(get_tweets)
while True:
    schedule.run_pending()
    schedule.run_all(delay_seconds=10)

    time.sleep(1)


# def main():
#     loop_query()

# if __name__ == "__main__":
#     main()
