import pymongo
from pymongo import MongoClient
import operator
import pandas as pd
import pickle
import json
import tweetanalyzer.tokenizer as tokenizer
import tweetanalyzer.utils as utils

READ_FROM_PICKLE=False
MONGO_DATABASE='twitter_zeynep'
MONGO_TABLE= 'userTweets_train'
MONGO_NEW_USER_TABLE= 'user_new_train'



def read_from_mongo():
    print("Initializing Mongo connection...")
    client = MongoClient('localhost:27017')
    db = client[MONGO_DATABASE]
    print("Initializing Mongo connection success.")

    counter_general = 0
    counter_new = 0

    #scrapped_tweets = db.MONGO_DATABASE.find()
    scrapped_tweets = db[MONGO_TABLE].find()

    user_dic ={}
    item_count=0
    for tweet in scrapped_tweets :
        user_retweeted_dic={}

        item_count+=1
        if(item_count % 100000==0):
            print("Processing item:"+str(item_count))

        if "reTweetedUserId" not in tweet.keys() :
            continue
        #tokenize hashtag
        retweeted_user_id = tweet['reTweetedUserId']
        retweeted_tweetid = tweet['reTweetedTweetId']
        retweet_count= tweet['retweetCount']

        #Get previous user retweeteds
        if retweeted_user_id in user_dic:
            user_retweeted_dic = user_dic[retweeted_user_id]

        if retweeted_tweetid in user_retweeted_dic:
            #print("This retweete has already been counted. Skipping...")
            continue
        else:
            #retweetcount and tweet count(1 by default)
            user_retweeted_dic[retweeted_tweetid]=(retweet_count,1)

        user_dic[retweeted_user_id]=user_retweeted_dic

    print("Now aggregating stats...")

    #finally aggregate retweet counts per user
    user_stats={}
    for user, user_tweeteds in user_dic.items():
        total_retweeted_count=0
        total_tweet_count=len(user_tweeteds)
        #print(user, user_tweeteds)

        for tweets,tweet_stats in user_tweeteds.items():
            total_retweeted_count+=tweet_stats[0]
        user_stats[user]= (total_tweet_count,total_retweeted_count)

    print("Aggregating stats complete...")


    df = pd.DataFrame.from_dict(user_stats, orient='index').reset_index()
    df.columns = ['user_id', 'total_tweet_count', 'total_retweeted_count']

    #Add new retweet ratio column
    df['retweet_ratio']= df['total_retweeted_count']/(df['total_tweet_count']+0.001)

    print("Number of tweets read from DB:"+str(item_count))

    return df



def stat_frequency():

    #Save DF to disk for quicker access
    if READ_FROM_PICKLE:
        print("Reading from pickle... ")
        df = pickle.load(open("tweet_pickle.p", "rb"))
    else:
        print("Reading from mongodb... ")
        df = read_from_mongo()
        pickle.dump(df, open("tweet_pickle.p", "wb"))

    print("Number of tweet hashtag loaded "+str(len(df)))


    print("user summary:")
    print("Count of distinct users:"+str(len(df)))

    print("DF sorted by tweet count")
    df = df.sort_values(by=['total_tweet_count'], ascending=[False])
    print(df.head(100))

    print("DF sorted by total_retweeted_count")
    df = df.sort_values(by=['total_retweeted_count'], ascending=[False])
    print(df.head(100))

    print("DF sorted by retweet_ratio")
    df = df.sort_values(by=['retweet_ratio'], ascending=[False])
    print(df.head(100))

    print("Persisting users to mongo")
    utils.persist_df_to_mongo(df, MONGO_DATABASE, MONGO_NEW_USER_TABLE)


def only_turkish_chars(s):
    try:
        s.encode("iso-8859-9")
        return True
    except:
        return False


def set_pandas_settings():
    pd.set_option('display.width', 320)
    pd.set_option('max_rows', 1000)


if __name__ == "__main__":
    set_pandas_settings()
    stat_frequency()