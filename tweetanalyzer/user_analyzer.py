import pymongo
from pymongo import MongoClient
import operator
import pandas as pd
import pickle
import json
import tweetanalyzer.tokenizer as tokenizer
import tweetanalyzer.utils as utils
import functools


READ_FROM_PICKLE=False
MONGO_DATABASE='twitter_zeynep'
MONGO_TABLE= 'users'
#MONGO_DATABASE='userTweets_test'



def read_from_mongo():
    print("Initializing Mongo connection...")
    client = MongoClient('localhost:27017')
    db = client[MONGO_DATABASE]
    print("Initializing Mongo connection success.")

    counter_general = 0
    counter_new = 0

    mongo_users = db[MONGO_TABLE].find()
    user_dic ={}
    item_count=0
    for user in mongo_users:
        item_count+=1
        if(item_count % 10000==0):
            print("Processing item:"+str(item_count))

        #print("Processing user with ID:"+str(user['name']))
        total_retweeteds=0
        if 'retweetedTweetCountPerTopic_Test' in user:
            #total_retweeteds = functools.reduce (lambda x,y: int(x['count'])+int(y['count']), user['retweetedTweetCountPerTopic_Test'])
            for topic in user['retweetedTweetCountPerTopic_Test']:
                total_retweeteds+=topic['count']

        total_tweets=user['tweet_count']
        total_followers = user['followersCount']
        tuple = (total_tweets,total_retweeteds,total_followers )
        user_dic[user['name']]=tuple

    df = pd.DataFrame.from_dict(user_dic, orient='index').reset_index()
    df.columns = ['name', 'total_tweets', 'total_retweeteds','total_followers']

    #Add new retweet ratio column
    df['retweet_ratio1']= df['total_retweeteds']/(df['total_tweets']+0.001)
    df['retweet_ratio2'] = df['total_retweeteds'] / (df['total_followers'] + 0.001)

    print("Number of users read from DB:"+str(item_count))

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


    print("User summary:")
    print("Count of distinct users:"+str(len(df)))
    print("Sum of tweets:" + str(sum(df['total_tweets'])))
    print("Sum of retweeteds:" + str(sum(df['total_retweeteds'])))

    df = sort_by_key(df,'total_tweets')
    df = sort_by_key(df, 'total_retweeteds')
    df = sort_by_key(df, 'retweet_ratio1')
    df = sort_by_key(df, 'retweet_ratio2')

    print("Persisting users to mongo")
    utils.persist_df_to_mongo(df,MONGO_DATABASE,"users_new")




def sort_by_key(df,key):
    print("DF sorted by key:"+key)
    df = df.sort_values(by=[key], ascending=[False])
    print(df.head(100))
    return df



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