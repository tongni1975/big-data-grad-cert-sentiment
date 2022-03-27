import requests
from streamlit.script_run_context import add_script_run_ctx
import threading
import json
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import altair as alt
import streamlit as st
import random
import re
from struct import Struct
import numpy as np
import pandas as pd
#from pandas import StringDtype
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F

from sqlalchemy import Integer, null
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob

import warnings
warnings.filterwarnings("ignore")

# streamlit

matplotlib.use('Agg')


thread = threading.Thread()
st.script_run_context.add_script_run_ctx(thread)
# thread.start()


def predict(tweet_text):
    # a line code to predict
    print(tweet_text.text)
    print("score: {}".format(random.random()))


httpRegex = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"


def preprocessing(df):
    df = df.na.replace('', None)
    df = df.na.drop()
    #df = df.withColumn('text_reformat', F.regexp_replace('text', r'@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+', ''))
    #df = df.withColumn('text_reformat', lit(' '.join(re.sub('(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|([RT])', ' ', df['text'].lower()).split())))
    # print("after preprocessing- {}".format(lines.iloc[0]))
    return df


def cleanTxt(text):
    text = re.sub('@[A-Za-z0-9]+', '', text)  # Removing @mentions
    text = re.sub('#', '', text)  # Removing '#' hash tag
    text = re.sub('RT[\s]+', '', text)  # Removing RT
    text = re.sub('https?:\/\/\S+', '', text)  # Removing hyperlink

    return text

# using textblob for sentiment


def senti_scoring(text):
    # return f'{TextBlob(text).sentiment.polarity:.2f}'
    return TextBlob(text).sentiment.polarity


def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity


def map_sentiment(score):
    return "Positive" if score > 0 else ("Negative" if score < 0 else "Neutral")


scoring_u = udf(senti_scoring, FloatType())
subject_u = udf(subjectivity_detection, FloatType())
tone_u = udf(map_sentiment, StringType())


def predict_sentiment(tw):
    tw = tw.withColumn("sentiment_score", scoring_u("text"))
    tw = tw.withColumn("subjectivity", subject_u("text"))
    tw = tw.withColumn("tone", tone_u("sentiment_score"))

    return tw


@udf
def plot_tones(df):
    # Plotting and visualizing the counts
    plt.title('Sentiment Analysis')
    plt.xlabel('Sentiment')
    plt.ylabel('Counts')
    df["tone"].value_counts().plot(kind='bar')
    plt.show()


sparkSession = SparkSession.builder.appName("bda_rr").config(
    "spark.driver.bindAddress", "127.0.0.1").getOrCreate()


@udf
def calc_move(df):
    # movement = sparkSession.sql("select tone from {} limit 1".format(df)
    move = df.select("tone").limit(1)

    return move


svr_add = "http://127.0.0.1:5000/ping"


def ping(msg):
    print(msg)
    requests.post(svr_add, json=msg)
    #requests.get(svr_add, params=msg)


# perform structured streaming (from excel)
# specify schema
# twitter_fields = ['id', 'text', 'author_id', 'created_at', 'geo']
twSchema = StructType([StructField('', IntegerType(), True),
                       StructField('id', StringType(), True),
                       StructField('text', StringType(), True),
                       StructField('author_id', StringType(), True),
                       StructField('created_at', TimestampType(), True),
                       StructField('geo', StringType(), True)])

tweets = sparkSession.readStream.format("csv").schema(twSchema) \
    .option("header", True).load("/Users/rayyang/repo/tweets_streaming/input")

tweets.isStreaming


# TBR test local df with streamit
data = [{"Pos": 11},
        {"Neg": 3},
        {"Neu": 16}]

tempDf = sparkSession.createDataFrame(data)
# convert spark df to pandas df
pdDf = tempDf.select("*").toPandas()

st.bar_chart(pdDf)

# define ML pipeline: clean out RT
tweets = tweets.select("author_id", "text", date_format(
    tweets.created_at, "yyyy-MM-dd HH:mm:ss").alias("date"))
tweets.createOrReplaceTempView("tweets_snapshot")
#sparkSession.sql("select * from tweets_snapshot").show(10)

# preprocessing items
cleaned_tweets = preprocessing(tweets)

# we skip the regular text process like tokenization/stop words removal/lemmentation, etc
# model function
cur_sentiment = predict_sentiment(cleaned_tweets)
#cur_sentiment = add_tone(cur_sentiment)

# TODO calculate overall sentiment for past hour by averaging
# avg_score = senti.groupBy("created_at").agg(
#     avg(senti.sentiment_score))


# visualise with streamit
st.title("Twitter Sentiment Analytics")
st.write("""Predict cryptocurrency price movement in tendom with text sentiments""")
# sidebar
st.sidebar.subheader('Select Cryptocurrency')
# output to sink
# cur_sentiment.writeStream.format("console").outputMode("append").start().awaitTermination()
# cur_sentiment.writeStream.format("json").option("path", "output").trigger(processingTime='2 seconds').outputMode(
#       "complete").option("checkpointLocation", "checkpoint/").start().awaitTermination()

# option 1 - write to kafka topic for web consumption
# cur_sentiment.writeStream.format("kafka").option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
#     .option("topic", "updates").start()

# option 2 - for each processing


def write_ext_stat(df, epochId):
    # Write row to storage
    # append(row)
    # output hour aggregate stats {date | tone | tone_count}
    tone_dist = df.groupBy(df.date, df.tone)\
        .agg(count("tone").alias("count")).sort("date", desc("count"))

    # tone_dist.select("tone").limit(1).show()

    # TODO conclude movement for that hour based on the distribution
    #tone_dist = tone_dist.withColumn("movement", calc_move(tone_dist))

    ping(tone_dist.toJSON().collect())
    # tone_dist.show()

    # merge data from all partitions
    df.coalesce(1).write.mode("append").json("hour_sentiment")
    df.coalesce(1).write.mode("append").csv("hour_sentiment")


def processRow(row):
    # row to json
    ping(row.asDict())


#query = cur_sentiment.writeStream.foreachBatch(write_ext_stat).start().awaitTermination()
query = cur_sentiment.writeStream.foreach(
    processRow).start().awaitTermination()
