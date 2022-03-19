#from ast import keyword
import tweepy
import schedule
import io
import sys
import time
import pickle
import os

keywords = ['bitcoin']

#Step 1: Creating a StreamListener
#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        print(status.text)


#Step 2: Creating a Stream

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

#Step 3: Starting a Stream
myStream.filter(track=keywords)

# handle error
class MyStreamListener(tweepy.StreamListener):

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False

        # returning non-False reconnects the stream, with backoff.