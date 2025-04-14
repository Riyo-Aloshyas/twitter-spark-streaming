from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json
from geopy.geocoders import Nominatim
from textblob import TextBlob
import re

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext

import os

geolocator = Nominatim(user_agent='tweet')

TCP_IP = 'localhost'
TCP_PORT = 9001
data = {}
data['tweet'] = []

def logToJsonFile(tweet,sentiment, country, state, lat, lon):
        
        with open('tweets.json', 'a+') as outfile: 

                        data['tweet'].append({
                        'tweet': tweet,        
                        'country': country,
                        'state': state,
                        'latitude': lat,
                        'longitude': lon,
                        'sentiment': sentiment
                        })

                        tweet = {
                               'tweet': tweet,        
                                'country': country,
                                'state': state,
                                'latitude': lat,
                                'longitude': lon,
                                'sentiment': sentiment 
                        }
                        
                        print(tweet)
                        json.dump(tweet, outfile, indent=4)
                        


def testDisplay(tweet):
    
    tweetData = tweet.split("::")

    if len(tweetData) > 1:
        text = tweetData[1]

        if float(TextBlob(text).sentiment.polarity) > 0.3:
            sentiment = "positive"
        elif float(TextBlob(text).sentiment.polarity) < -0.3:
            sentiment = "negative"
        else:
            sentiment = "neutral"

        try:
            location = geolocator.geocode(tweetData[0], addressdetails=True)
            lat = location.raw['lat']
            lon = location.raw['lon']
            state = location.raw['address']['state']
            country = location.raw['address']['country']
        except:
            lat=lon=state=country=None

        logToJsonFile(text, sentiment, country, state, lat, lon)    
        
        print("\n\n=================================================")
        print("Tweet: ", text)
        print("Lat: ", lat)
        print("Lon: ", lon)
        print("State: ", state)
        print("Country: ", country)
        print("Sentiment: ", sentiment)

        

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

dataStream.foreachRDD(lambda rdd: rdd.foreach(testDisplay))

#df = sqlContext.read.option("multiline", "true").json("tweets.json")
# print("\n\n\n\n\n\n This SCheeememammama")



ssc.start()
ssc.awaitTermination()