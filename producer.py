
from pyspark.streaming.kafka import KafkaUtils

from kafka import SimpleProducer, KafkaClient

from kafka import KafkaProducer

from operator import add

import sys

import json

from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream



def read_credentials():

    creds = "/home/marksukhram595/credentials.json"

    try:

        with open(creds) as data_file:

            return json.load(data_file)

    except:

        print ("Error loading credentials")

        return None



def producer1():

    spark_context = SparkContext(appName="First Group Producer")

    spark_sc = StreamingContext(spark_context, 60)

    brokers = "localhost:9092"

    kvs = KafkaUtils.createDirectStream(spark_sc, ["test"], {"metadata.broker.list": brokers})

    kvs.foreachRDD(send)

    producer.flush()

    spark_sc.start()

    spark_sc.awaitTermination()



def send(message):

    iterator = twitter_stream.statuses.sample()
    
    count=0

    for tweet in iterator:

        producer.send('twitter', bytes(json.dumps(tweet, indent=6), "ascii"))

        count+=1

        if(count==1000):

            break



if __name__ == "__main__":

    print("Stating to read tweets")

    credentials = read_credentials()

    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])

    twitter_stream = TwitterStream(auth=oauth)

    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    producer1()
                                                                                                                                                                                          89,1          Bot





