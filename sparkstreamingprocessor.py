import random
from struct import Struct
#from pandas import StringDtype
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F

from sqlalchemy import Integer, null
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob


def predict(tweet_text):
    # a line code to predict
    print(tweet_text.text)
    print("score: {}".format(random.random()))

httpRegex = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"

def preprocessing(df):
    df = df.na.replace('', None)
    df = df.na.drop()
    df = df.withColumn('text_reformat', F.regexp_replace('text', r'@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+', ''))

    # print("after preprocessing- {}".format(lines.iloc[0]))
    return df

# using textblob for sentiment
def senti_scoring(text):
    return TextBlob(text).sentiment.polarity
    #return random.random()

def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity

scoring_u = udf(senti_scoring, StringType())
subject_u = udf(subjectivity_detection, StringType())

def predict_sentiment(tw):
    tw = tw.withColumn("sentiment_score", scoring_u("text"))
    tw = tw.withColumn("subjectivity", subject_u("text"))

    return tw

# conf = SparkConf()
# conf.setAppName("bdc_streaming")

# sc = SparkContext(conf=conf)
# sc.setLogLevel("ERROR")

sparkSession = SparkSession.builder.appName("bda_rr").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()

# perform structured streaming (from excel)
# specify schema
# twitter_fields = ['id', 'text', 'author_id', 'created_at', 'geo']
twSchema = StructType([StructField('', IntegerType(), True),
                       StructField('id', StringType(), True),
                       StructField('text', StringType(), True),
                       StructField('author_id', StringType(), True),
                       StructField('created_at', DateType(), True),
                       StructField('geo', StringType(), True)])

tweets = sparkSession.readStream.format("csv").schema(twSchema) \
    .option("header", True).load("/Users/rayyang/repo/tweets_streaming/")

tweets.isStreaming

# define ML pipeline: clean out RT
tweets = tweets.select("author_id", "text", date_format(tweets.created_at, "yyyy-MM-dd").alias("date"))
tweets.createOrReplaceTempView("tweets_snapshot")
#sparkSession.sql("select * from tweets_snapshot").show(10)

# preprocessing items
cleaned_tweets = preprocessing(tweets)


# we skip the regular text process like tokenization/stop words removal/lemmentation, etc
# model function
# tweet_text.writeStream.foreach(predict).start()
#senti2 = cleaned_tweets.withColumn("sentiment_score", scoring_udf(cleaned_tweets['text_reformat']))
#tweets = preprocessing(tweets)
cur_sentiment = predict_sentiment(cleaned_tweets)

# TODO calculate overall sentiment for this period by averaging
# avg_score = senti.groupBy("created_at").agg(
#     avg(senti.sentiment_score))

# output to sink
cur_sentiment.writeStream.format("console").outputMode("append").start().awaitTermination()
# senti.writeStream.format("json").option("path", "output").trigger(processingTime='2 seconds').outputMode(
#     "append").option("checkpointLocation", "checkpoint/").start().awaitTermination()

