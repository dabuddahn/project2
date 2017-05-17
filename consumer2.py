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
        print ("Cannot load credentials.")
        return None

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def consumer():
    context = StreamingContext(spark_context, 60)
    dStream = KafkaUtils.createDirectStream(context, ["twitter2"], {"metadata.broker.list": "localhost:9092"})
    
    
    #Start Question 1
    dStream.foreachRDD(p1)
    #End Question 1
    
    #Start Question 2
    dStream.foreachRDD(p2)
    #End Question 2

    context.start()
    context.awaitTermination()
    
def p1(time,rdd):

    #remove field [0] -> none and convert data str in dict
    rdd=rdd.map(lambda x: json.loads(x[1]))
    records=rdd.collect()    
    
    records = [element["user"]["screen_name"] for element in records if "user" in element] # Select only screenname part

    if not records:
        print("Empty List")
    else:
        rdd = spark_context.parallelize(records)

        spark = getSparkSessionInstance(rdd.context.getConf())
        # Convert RDD[String] to RDD[Row] to DataFrame
        hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(screenname=x, time_stamp=time)))
        hashtagsDataFrame.createOrReplaceTempView("screennames")
        hashtagsDataFrame = spark.sql("select screenname, count(*) as total, time_stamp from screennames group by screenname, time_stamp order by total desc limit 5")
        hashtagsDataFrame.write.mode("append").saveAsTable("screennames_table")

        #Insert code de graph

    print(time)
  
def p2(time,rdd):
    rdd=rdd.map(lambda x: json.loads(x[1]))
    records=rdd.collect()
    
    records = [element["text"] for element in records if "text" in element]
    
    if not records:
        print("Empty List")
    else:
        # new_records = list(map(lambda x: filter(lambda x: x == "a", element for element in records)), records)
        rdd = spark_context.parallelize(records)
        spark = getSparkSessionInstance(rdd.context.getConf())
        rdd = rdd.map(lambda x: x.split()).flatMap(lambda x: x).map(lambda x: x.lower())
        rdd = rdd.filter(lambda x: x == "trump" and x == "maga" and x == "dictator" and x == "impeach" and x == "drain" and x == "swamp")
        if rdd.count() > 0:
            keywordDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(keyword=x, time_stamp=time)))
            keywordDataFrame.createOrReplaceTempView("specific_keywords")
            keywordDataFrame = spark.sql("select keyword, count(*) as total, time_stamp from specific_keywords group by keyword, time_stamp order by total")
            keywordDataFrame.write.mode("append").saveAsTable("specific_keywords_table")

if __name__ == "__main__":
    print("Stating to read tweets")
    credentials = read_credentials() 
    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    twitter_stream = TwitterStream(auth=oauth)
    spark_context = SparkContext(appName="Second Group Consumer")
    checkpointDirectory = "/checkpoint"
    consumer()

