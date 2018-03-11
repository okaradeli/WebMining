import tweepy
from tweepy import User
from tweepy import TweepError
import pymongo
from pymongo import MongoClient
import pprint
import datetime
import time
import sys
from TweetScraper.twitterapi import *
from TweetScraper.twitterapi.tweet import Tweet
import TweetScraper.twitterapi.twitter_api_cfg as twitter_api_config


def main():
  

    try:
        print("Logging to Twitter")
        auth = tweepy.OAuthHandler(twitter_api_config.consumer_key, twitter_api_config.consumer_secret)
        auth.set_access_token(twitter_api_config.access_token, twitter_api_config.access_token_secret)
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        print("Logging to Twitter success.")

        print("Initializing Mongo connection...")
        client = MongoClient('localhost:27017')
        db = client.TweetScraper
        print("Initializing Mongo connection success.")

        counter_general = 0
        counter_new = 0

        cursor = db.tweet.find()
        for res in cursor:
            try:
                counter_general = counter_general + 1
                # this will print the item as a dictionary
                if "api_res" in res.keys():
                    print("This record has been processed before. Skipping...")
                    continue

                print("New record. ID="+res["ID"])
                counter_new = counter_new + 1

                if (counter_new % 1000 == 0 ):
                    print ("success counter:"+str(counter_general)+" general counter:"+str(counter_general))

                t = api.get_status(res["ID"], tweet_mode='extended')
                print("Parsing tweet with ID:" + res["ID"])
                tweet = Tweet(t)
                db_tweet_json = tweet.get_tweet_as_db_json()
                db.tweet.update({"ID": res["ID"]}, {"$set": db_tweet_json})

            except TweepError as e:
                if e.api_code == 63:
                    print('Skipping tweet, account is suspended for ID:', res["ID"], ',', e)
                    db.tweet.update({"ID": res["ID"]}, {"$set": {"api_res": '1'}})
                elif e.api_code == 144:
                    print('Skipping tweet, no status related to this account for ID:', res["ID"], ',',e)
                    db.tweet.update({"ID": res["ID"]}, {"$set": {"api_res": '2'}})
                else:
                    print('Skipping tweet, not known error, for ID:', res["ID"], ',', e)
                    db.tweet.update({"ID": res["ID"]}, {"$set": {"api_res": '3'}})
            except Exception as exception:
                print('Oops!  An error occurred.  Try again...', exception)
                db.tweet.update({"ID": res["ID"]}, {"$set": {"api_res": '4'}})
                raise exception

    except Exception as exception:
        print('Oops!  An error occurred.  Try again...', exception)
        raise exception


if __name__ == "__main__":
    main()