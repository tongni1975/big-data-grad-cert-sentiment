#! /bin/bash

exec python -m flask run --host=0.0.0.0 & exec python twitter_tweepy.py & exec python streamprocessor.py 
