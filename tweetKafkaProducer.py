import json, inspect

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import threading, logging, time
from kafka import KafkaProducer
import string
from config import *


# logging.basicConfig(level=logging.DEBUG,format='(%(threadName)-10s) %(message)s',)

keys_c = ["careerarc","want","good","tokyo","job","london","wind","rain","love","turkey"]
mytopic = 'tweet'# ex. 'twitterstream', or 'test' ...


producer = KafkaProducer(bootstrap_servers=['54.187.126.150:9092'])
######################################################################
#Create a handler for the streaming data that stays open...
######################################################################

class TweetStreamListener(StreamListener):

   
    def on_status(self, status):
     
        if status.geo ==None : return True
        # print [name for name,thing in inspect.getmembers(status)]
        # print status.timestamp_ms, status.id,status.coordinates["coordinates"][0],status.text.encode('utf-8')
        twl=[]
        data={}
        data["keyw"]="none" 
        msg = status.text.encode('utf-8')

        for k in keys_c:
            if k in msg:
                data["keyw"]=k
                break

 
        data["tid"]= status.id
        data["latitude"]= status.coordinates["coordinates"][1]
        data["longitude"]= status.coordinates["coordinates"][0]
        data["epoch"]= int(status.timestamp_ms)/1000
        data["status"]= msg
        

        twl.append(data)
        print twl
        try:
            producer.send(mytopic, json.dumps(twl))
        except Exception as e:
            print e
            return True
        
        return True
       
    ######################################################################
    #Supress Failure to keep demo running... In a production situation 
    #Handle with seperate handler
    ######################################################################
 
    def on_error(self, status_code):

        print('Got an error with status code: ' + str(status_code))
        return True # To continue listening
 
    def on_timeout(self):

        print('Timeout...')
        return True # To continue listening

######################################################################
#Main Loop Init
######################################################################


if __name__ == '__main__':
    
    listener = TweetStreamListener()

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, listener)
    # keys = ["careerarc","want","good","tokyo","job","london","wind","rain","love","turkey"]

    ######################################################################
    #Sample delivers a stream of 1% (random selection) of all tweets
    ######################################################################
    # stream.sample()

    stream.filter(languages=["en"],track= keys_c, stall_warnings=True)
