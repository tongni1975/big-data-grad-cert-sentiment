#! /bin/bash

exec python app.py & exec python twitter_tweepy.py & exec python streamprocessor.py 
