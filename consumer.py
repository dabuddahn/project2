  GNU nano 2.5.3                                                       File: consumer.py                                                                                                                    

from pyspark import SparkConf, SparkContext

from pyspark.streaming import StreamingContext

from pyspark.streaming.kafka import KafkaUtils

from kafka import SimpleProducer, KafkaClient

from kafka import KafkaProducer

from operator import add

import sys

from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream

from pyspark.sql import Row, SparkSession

try:

    import json

except ImportError:

    import simplejson as json


def read_credentials():

    creds = "/home/marksukhram595/credentials.json"

    try:

        with open(creds) as data_file:

            return json.load(data_file)

    except:

        print ("Cannot load credentials")

        return None



def getSparkSessionInstance(sparkConf):

    if ('sparkSessionSingletonInstance' not in globals()):

        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()

    return globals()['sparkSessionSingletonInstance']
    
    def consumer():

    #context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)

    context = StreamingContext(spark_context, 10)

    dStream = KafkaUtils.createDirectStream(context, ["twitter"], {"metadata.broker.list": "localhost:9092"})

    

    

    #Start Question 1

    dStream.foreachRDD(p1)

    #End Question 1

    

    #Start Question 2

    dStream.foreachRDD(p2)

    #End Question 2



    context.start()

    context.awaitTermination()



def p1(time,rdd):

    #remove field [0]

    rdd=rdd.map(lambda x: json.loads(x[1]))

    records=rdd.collect()    

    records = [element["entities"]["hashtags"] for element in records if "entities" in element] # Select only hashtags part

    records = [x for x in records if x] # Remove empty hashtags

    records = [element[0]["text"] for element in records] # Saving hashtag text in records

    if not records:

        print("Empty List")

    else:

        rdd = spark_context.parallelize(records)

        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame

        hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(hashtag=x, time_stamp=time)))

        hashtagsDataFrame.createOrReplaceTempView("hashtags")

        hashtagsDataFrame = spark.sql("select hashtag, count(*) as total, time_stamp from hashtags group by hashtag, time_stamp order by total desc limit 5")

        hashtagsDataFrame.write.mode("append").saveAsTable("hashtag_table")

    

    print(time)



def p2(time,rdd):

    rdd=rdd.map(lambda x: json.loads(x[1]))

    records=rdd.collect()

    

    records = [element["text"] for element in records if "text" in element]

    

    if not records:

        print("Empty List")

    else:
        rdd = spark_context.parallelize(records)
        spark = getSparkSessionInstance(rdd.context.getConf())
        rdd = rdd.map(lambda x: x.split()).flatMap(lambda x: x).map(lambda x: x.lower())
        rdd = rdd.filter(lambda x: x != "a" and x != "and" and x != "an" and x != "are" and x != "as" and x != "at" and x != "be" and x != "by" and x != "for" and x != "from" and x != "has" and x != "he"
        and x != "in" and x != "is" and x != "it" and x != "its" and x != "of" and x != "on" and x != "that" and x != "the" and x != "to" and x != "was" and x != "were" and x != "will" and x != "with")
        keywordDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(keyword=x, time_stamp=time)))
        keywordDataFrame.createOrReplaceTempView("keywords")
        keywordDataFrame = spark.sql("select keyword, count(*) as total, time_stamp from keywords group by keyword, time_stamp order by total desc limit 5")
        keywordDataFrame.write.mode("append").saveAsTable("keywords_table")

if __name__ == "__main__":

    print("Stating to read tweets")

    credentials = read_credentials() 

    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])

    twitter_stream = TwitterStream(auth=oauth)

    spark_context = SparkContext(appName="First Group Consumer")

    checkpointDirectory = "/checkpoint"

    consumer()

