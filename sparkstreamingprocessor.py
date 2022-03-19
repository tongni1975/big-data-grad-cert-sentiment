import random
from struct import Struct
from pandas import StringDtype
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


def preprocessing(df):
    df = df.na.replace('', None)
    df = df.na.drop()
    df = df.withColumn('text_reformat', F.regexp_replace('text', r'http\S+', ''))
    df = df.withColumn('text_reformat', F.regexp_replace('text', '@\w+', ''))
    df = df.withColumn('text_reformat', F.regexp_replace('text', '#', ''))
    df = df.withColumn('text_reformat', F.regexp_replace('text', 'RT', ''))
    df = df.withColumn('text_reformat', F.regexp_replace('text', ':', ''))

    # print("after preprocessing- {}".format(lines.iloc[0]))
    return df

# using textblob for sentiment
def senti_scoring(text):
    return TextBlob(text).sentiment.polarity

scoring_udf = udf(lambda i: senti_scoring(i), StringType())

conf = SparkConf()
conf.setAppName("bdc_streaming")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

sparkSession = SparkSession.builder.appName("bda_rr").config(
    "spark.driver.bindAddress", "127.0.0.1").getOrCreate()

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
    .option("header", True).load("C:/Users/yangw/repo/bda_crypto_analytics")

tweets.isStreaming

# define ML pipeline: clean out RT
tweets.select("text", "author_id", date_format(tweets.created_at, "yyyy MM dd"))

cleaned_tweets = preprocessing(tweets)

# we skip the regular text process like tokenization/stop words removal/lemmentation, etc
# model function
# tweet_text.writeStream.foreach(predict).start()
senti2 = cleaned_tweets.withColumn("sentiment_score", scoring_udf(cleaned_tweets['text_reformat']))
senti = tweets.withColumn("sentiment_score", lit(1))

# TODO calculate overall sentiment for this period by averaging
# avg_score = senti.groupBy("created_at").agg(
#     avg(senti.sentiment_score))

# output to sink
senti2.writeStream.format("console").outputMode("append").start().awaitTermination()
# senti.writeStream.format("json").option("path", "output").trigger(processingTime='2 seconds').outputMode(
#     "append").option("checkpointLocation", "checkpoint/").start().awaitTermination()
