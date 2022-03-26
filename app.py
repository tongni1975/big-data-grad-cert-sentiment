from sqlite3 import Timestamp
from flask import Flask
from flask import request, jsonify, redirect, render_template, url_for
from flask_sqlalchemy import SQLAlchemy
from pytz import timezone
from sympy import true
import time
from datetime import datetime, timedelta
from sqlalchemy.sql import func
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import TIMESTAMP


import json

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class AlcDateTime(db.TypeDecorator):
    impl = db.DateTime

    def process_bind_param(self, value, dialect):
        if type(value) is str:
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value


class DoubleTimestamp(db.TypeDecorator):
    impl = db.DateTime

    # def __init__(self):
    #     db.TypeDecorator.__init__(self, as_decimal=False)

    def process_bind_param(self, value, dialect):
        # return value.timestamp() * 1000
        return (value - datetime(1970, 1, 1)) / timedelta(seconds=1)

    def process_result_value(self, value, dialect):
        return datetime.datetime.utcfromtimestamp(value / 1000)


class Tweet(db.Model):
    author_id = db.Column(db.String(512))
    text = db.Column(db.String(512), primary_key=True)
    date = db.Column(AlcDateTime)
    sentiment_score = db.Column(db.Float())
    subjectivity = db.Column(db.Float())
    tone = db.Column(db.String(8))


class as_utc(GenericFunction):
    type = AlcDateTime
    package = "time"
    inherit_cache = True


@app.route("/")
def home():
    # past 1 hour tweets
    past_hour = datetime.now() - timedelta(hours=8)
    # print("past hour {}".format(past_hour))
    tweets = Tweet.query.filter(
        Tweet.date >= past_hour).order_by(Tweet.date.desc()).all()

    positive_tweets_for_plot = Tweet.query.with_entities(Tweet.date, Tweet.sentiment_score).filter(
        Tweet.date >= past_hour, Tweet.tone == 'Positive').order_by(Tweet.date.desc()).all()

    negative_tweets_for_plot = Tweet.query.with_entities(Tweet.date, Tweet.sentiment_score).filter(
        Tweet.date >= past_hour, Tweet.tone == 'Negative').order_by(Tweet.date.desc()).all()

    neutral_tweets_for_plot = Tweet.query.with_entities(Tweet.date, Tweet.sentiment_score).filter(
        Tweet.date >= past_hour, Tweet.tone == 'Neutral').order_by(Tweet.date.desc()).all()

    # print(json.dumps(positive_tweets_for_plot))
    neg = []
    for tw in negative_tweets_for_plot:
        neg.append([x for x in tw])  # or simply data.append(list(row))

    pos = []
    for tw in positive_tweets_for_plot:
        # for ( x in tw){
        #     fill = time.mktime(t.timetuple())*1000
        # }
        pos.append([x for x in tw])

    ner = []
    for tw in neutral_tweets_for_plot:
        ner.append([x for x in tw])

    print(neg)

    return render_template("base.html", tweetList=tweets, posTweets=pos, negTweets=neg, nerTweet=ner)


@ app.route("/ping", methods=['POST', 'GET'])
def listen_agg_stat():
    stat_msg = request.get_json()
    print(stat_msg)

    latest_tweet = Tweet(**stat_msg)
    # no primary key
    db.session.add(latest_tweet)
    db.session.commit()

    return redirect(url_for("home"))


if __name__ == "__main__":
    db.create_all()
    app.run(debug=True)
