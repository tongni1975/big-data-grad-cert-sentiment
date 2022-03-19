from struct import Struct
from pandas import StringDtype

from sqlalchemy import Integer, null
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#import os
#os.environ['PYSPARK_SUBMIT_ARGS'] = "--master mymaster --total-executor 2 --conf "spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.mycorp.com-Dhttp.proxyPort=1234 -Dhttp.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 -Dhttps.proxyHost=proxy.mycorp.com -Dhttps.proxyPort=1234 -Dhttps.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 pyspark-shell"

sparkSession = SparkSession.builder.appName("bda_rr").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()

# perform structured streaming (from excel)
# specify schema
# twitter_fields = ['id', 'text', 'author_id', 'created_at', 'geo']
twSchema = StructType([StructField('serial', IntegerType(), True), StructField('id', StringType(), True), StructField('text', StringType(), True),\
    StructField('author_id', StringType(), True), StructField('created_at', DateType(), True), \
        StructField('geo', StringType(), True)])
tweet = sparkSession.readStream.format("csv").schema(twSchema)\
    .option("header", True).load("/Users/rayyang/repo/bda_twits/")

tweet.isStreaming

# define ML pipeline: clean out RT
tweet.select("id", "text", "author_id", date_format(tweet.created_at, "yyyy MM dd")).where(~(tweet.text.eqNullSafe("RT")))
#tweet['created_at'] = tweet['created_at'].dt.strftime('%m/%d/%Y')

# we skip the regular text process like tokenization/stop words removal/lemmentation, etc
# model function
def predict(tweet_text):
    # a line code to predict
    return 1.0

senti = tweet.withColumn("sentiment_score", lit(predict(tweet.text)))

#TODO calculate overall sentiment for this period by averaging
# avg_score = senti.groupBy("created_at").agg(
#     avg(senti.sentiment_score))

# output to sink
#senti.writeStream.format("console").outputMode("append").start().awaitTermination()  

senti.writeStream.format("csv").option("path", "output.csv").outputMode("append").option("checkpointLocation", "/").start().awaitTermination()
#tweet.writeStream.format("memory").queryName("counts").outputMode("complete").start()

# Streaming can be from socket - readStream.format("socket").option("host", "localhost").option("port", 8888)

